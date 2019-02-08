package core;

import core.graphPipeline.SDF.SDFGraph;
import core.graphPipeline.basic.*;
import core.graphPipeline.graphSymbol.Symbol;
import core.graphPipeline.graphSymbol.SymbolTable;
import core.pipelineOptimizer.*;
import featurePipeline.InfusionStage.InfusionStage;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import traceability.components.abstractComponents.TraceArtifact;
import traceability.components.abstractComponents.TraceLink;

import java.util.*;

import static core.pipelineOptimizer.PipelineOptimizer.createUniqueNewFieldName;
import static org.apache.spark.sql.functions.lit;


/**
 * SparkTraceTask (STT) is a complete runnable trace task, the SDF will generate the required field for DDF,
 * and DDF provide feature vector for the Model. SDFs, DDF, and Model are represented as computation graphs.
 * When a child STT C_STT  is nested in a parent STT P_STT, its SDFs will be collected by parent STT. Only a
 * top level STT will execute SDFs and do join operation.
 * <p>
 * The namespace of all sourceSDF/targetSDF is global, this is because these graphs get input directly
 * from Dataframe.
 */
public class SparkTraceTask extends SGraph {
    private boolean isInitialed = false; //record whether the task is init or not
    //Symbol name for id column which can be configured in different places.
    private String sourceIdCol, targetIdCol;
    private SparkSession sparkSession;
    private SDFGraph sdfGraph;
    private SGraph ddfGraph;
    private TransparentSNode infusionNode;
    private PipelineModel taskModel;


    public SparkTraceTask(SparkSession sparkSession, SDFGraph sdfGraph, SGraph ddfGraph, String sourceIdCol, String targetIdCol) {
        super();
        this.sparkSession = sparkSession;
        this.sdfGraph = sdfGraph;
        this.ddfGraph = ddfGraph;
        this.infusionNode = new TransparentSNode(new InfusionStage(sdfGraph, ddfGraph), "infusion_" + UUID.randomUUID());
        addNode(sdfGraph);
        addNode(ddfGraph);
        addNode(this.infusionNode);
        this.sourceIdCol = sourceIdCol;
        this.targetIdCol = targetIdCol;
    }

    /**
     * Connect the SDF and DDF. (for one sub STT)
     * 1. Add all the symbols to the DDF input fields that the sub STT have created in SDF
     * 2. Connect the addedSDFSymbols in SDF to the newly created DDF symbols.
     *
     * @param addedSDFSymbols
     * @param SDF
     * @param DDF
     * @param SDFSymbolMap
     * @return
     * @throws Exception
     */
    private List<String> connectSDFToDDF(List<Symbol> addedSDFSymbols, SGraph SDF, SGraph DDF, Map<String, String> SDFSymbolMap) throws Exception {
        List<String> addedDDFSymbolNames = new ArrayList<>();
        for (Symbol SDFAddedField : addedSDFSymbols) {
            String fromSymbolName = SDFAddedField.getSymbolName();
            String toSymbolName = SDFAddedField.getSymbolName();

            DDF.addInputField(toSymbolName);
            this.connectSymbol(SDF, fromSymbolName, DDF, toSymbolName);//TODO toSymbolName should be unique name
            addedDDFSymbolNames.add(SDFAddedField.getSymbolName());
            SDFSymbolMap.put(toSymbolName, fromSymbolName);
        }
        return addedDDFSymbolNames;
    }

    private void mergeSDF(SGraph parentSDF, SGraph childSDF, List<GraphHierarchyTree> path) throws Exception {
        List<Symbol> addedOutputFields = new ArrayList<>();
        //Trace the added outputFields to inner STT's DDF inputs
        //Key is the symbol name in current graph, value is the symbol name in SDF. Value will not change once created..
        Map<String, String> SDFSymbolMap = new HashMap<>();
        parentSDF.addNode(childSDF); //Move the childSDF to parent SDF

        //The input of childSTT are from parent SDF and passed to childSTT through the ouputfiled of parent SDF.
        //Once the childSDF is moved into parent SDF, the input provider should connectSymbol the childSDF directly


        //connectSymbol the childSDF to childDDF
        for (IOTableCell outputCell : childSDF.getOutputTable().getCells()) {
            String addedOutputFieldName = createUniqueNewFieldName(outputCell);
            Symbol newParentOutputFiled = new Symbol(parentSDF, addedOutputFieldName);
            parentSDF.addOutputField(newParentOutputFiled);
            parentSDF.connectSymbol(childSDF, outputCell.getFieldSymbol().getSymbolName(), parentSDF.sinkNode, addedOutputFieldName);
            addedOutputFields.add(newParentOutputFiled);
        }

        //TODO fix this
        for (IOTableCell inputCell : childSDF.getInputTable().getCells()) {
            String addedInputFieldName = inputCell.getFieldSymbol().getSymbolName(); //This name should not change
            Symbol newParentInputField = new Symbol(parentSDF, addedInputFieldName);
            parentSDF.addInputField(newParentInputField);
            parentSDF.connectSymbol(parentSDF.sourceNode, addedInputFieldName, childSDF, addedInputFieldName);
        }

        SGraph lastGraph = path.get(0).getNodeContent();
        List<String> addedSymbolNames = connectSDFToDDF(addedOutputFields, parentSDF, lastGraph, SDFSymbolMap);
        for (int i = 1; i < path.size(); i++) {
            SGraph curGraph = path.get(i).getNodeContent();
            if (curGraph instanceof SparkTraceTask) {
                //if current level is a STT connectSymbol the STT input to its DDF input
                SparkTraceTask innerTask = (SparkTraceTask) curGraph;
                Map<String, String> reversedSDFSymbolMap = reverseMapKeyValue(SDFSymbolMap);

                List<IOTableCell> subTaskSDFIOCells = innerTask.getSdfGraph().getOutputTable().getCells();
                //The innerSDF keeps the connection to inner DDF, use this connection to connectSymbol innerSTT's sourceNode and innerDDF
                for (IOTableCell cell : subTaskSDFIOCells) {
                    List<IOTableCell> connectedCells = cell.getOutputTarget();
                    IOTableCell parentSDFOutputCell = null;
                    IOTableCell innerTaskDDFInputCell = null;
                    for (IOTableCell conCell : connectedCells) {
                        if (conCell.getParentTable().getContext().equals(parentSDF.sinkNode)) {
                            parentSDFOutputCell = conCell;
                        } else if (conCell.getParentTable().getContext().equals(innerTask.getDdfGraph().sourceNode)) {
                            innerTaskDDFInputCell = conCell;
                        }
                    }
                    String innerSTTInputFiledSymbolName = reversedSDFSymbolMap.get(parentSDFOutputCell.getFieldSymbol().getSymbolName());
                    String innerDDFInputFiledSymbolName = innerTaskDDFInputCell.getFieldSymbol().getSymbolName();
                    innerTask.connectSymbol(innerTask.sourceNode, innerSTTInputFiledSymbolName, innerTask.getDdfGraph(), innerDDFInputFiledSymbolName);
                }
            } else {
                //Create input field for current graph, connectSymbol the input from parent graph to current graph
                for (String symbolName : addedSymbolNames) {
                    String addInputFieldName = symbolName; //TODO unique name
                    String symbolNameInSDF = SDFSymbolMap.get(symbolName);
                    SDFSymbolMap.remove(symbolName);
                    SDFSymbolMap.put(addInputFieldName, symbolNameInSDF);
                    curGraph.addInputField(addInputFieldName);
                    lastGraph.connectSymbol(lastGraph.sourceNode, symbolName, curGraph, addInputFieldName);
                }
            }
            lastGraph = curGraph;
        }
    }

    /**
     * Modify the infusion node to connectSymbol the sdf and ddf.
     */
    private void infuse() throws Exception {
        for (IOTableCell sdfOut : sdfGraph.getOutputTable().getCells()) {
            Symbol infusionIn = new Symbol(infusionNode, sdfOut.getFieldSymbol().getSymbolName() + "_in");
            Symbol infusionOut = new Symbol(infusionNode, sdfOut.getFieldSymbol().getSymbolName() + "_out");
            infusionNode.addInputField(infusionIn);
            infusionNode.addOutputField(infusionOut);
            this.connect(sdfOut.getFieldSymbol(), infusionIn);
            sdfOut.setRemovable(false);
            for (IOTableCell ddfReceiver : sdfOut.getOutputTarget()) {
                connect(infusionOut, ddfReceiver.getFieldSymbol());
                disconnect(sdfOut.getFieldSymbol(), ddfReceiver.getFieldSymbol());
            }
        }
    }

    /**
     * Collect SDFs from child STTs by scanning the nodes with BFS. If a STT found then register it; if
     * a Graph found, add all its nodes to search list; If a node found ignore it.
     * Take O(n) time.
     */
    public void initSTT() throws Exception {
        infuse();
        GraphHierarchyTree ght = new GraphHierarchyTree(null, this);
        if (!isInitialed) {
            isInitialed = true;
            List<Vertex> nodes = ddfGraph.getNodes();
            for (Vertex node : nodes) {
                if (node instanceof SparkTraceTask) {
                    SparkTraceTask subTask = (SparkTraceTask) node;
                    //Make sure the child STT have merged all inner STTs
                    subTask.initSTT();

                    //Find a path from parent STT's DDF graph to subTask in GHT, the first node in path is parent DDF
                    GraphHierarchyTree sparkTaskTreeNode = ght.findNode(subTask);
                    GraphHierarchyTree DDFTreeNode = ght.findNode(ddfGraph);
                    List<GraphHierarchyTree> path = new ArrayList<>();
                    ght.findPath(DDFTreeNode, sparkTaskTreeNode, path);

                    //Merge the childSTT to the parent STT
                    mergeSDF(sdfGraph, subTask.getSdfGraph(), path);
                }
            }
        }
    }

    @Override
    public Pipeline toPipeline() throws Exception {
        ((InfusionStage) infusionNode.getSparkPipelineStage()).setSourceIdCol(getSourceIdCol());
        ((InfusionStage) infusionNode.getSparkPipelineStage()).setTargetIdCol(getTargetIdCol());
        return super.toPipeline();
    }

    /**
     * Combine the schema of given dataset together, add null to the field which have no value;
     * If source and target have columns with same name then the columns will be regarded as same column and union
     * together.
     *
     * @param sourceArtifacts
     * @param targetArtifacts
     * @return
     */
    private Dataset<Row> UnionSourceAndTarget(Dataset<? extends TraceArtifact> sourceArtifacts,
                                              Dataset<? extends TraceArtifact> targetArtifacts) {
        List<String> sourceCols = Arrays.asList(sourceArtifacts.columns());
        List<String> targetCols = Arrays.asList(targetArtifacts.columns());

        //ensure no duplicated column name in input
        Set<String> intersect1 = new HashSet<>(sourceCols);
        Set<String> intersect2 = new HashSet<>(targetCols);
        intersect1.retainAll(intersect2);
        assert intersect1.size() == 0;

        Dataset<Row> sourceDF = sourceArtifacts.toDF();
        Dataset<Row> targetDF = targetArtifacts.toDF();
        for (String sourceCol : sourceCols) {
            targetDF = targetDF.withColumn(sourceCol, lit(null));
        }
        for (String targetCol : targetCols) {
            sourceDF = sourceDF.withColumn(targetCol, lit(null));
        }
        Dataset<Row> mixed = sourceDF.unionByName(targetDF);
        return mixed;
    }


    /**
     * Execute this STT as a top level STT.
     */
    public void train(Dataset<? extends TraceArtifact> sourceArtifacts,
                      Dataset<? extends TraceArtifact> targetArtifacts,
                      Dataset<? extends TraceLink> goldenLinks) throws Exception {
        Dataset<Row> combinedDataset = UnionSourceAndTarget(sourceArtifacts, targetArtifacts);
        setTrainingFlag(true);
        if (goldenLinks != null) {
            setGoldenLinks(goldenLinks.toDF());
        }
        taskModel = this.toPipeline().fit(combinedDataset);
    }

    public Dataset<Row> trace(Dataset<? extends TraceArtifact> sourceArtifacts,
                              Dataset<? extends TraceArtifact> targetArtifacts) {
        Dataset<Row> combinedDataset = UnionSourceAndTarget(sourceArtifacts, targetArtifacts);
        setTrainingFlag(false);
        Dataset<Row> result = taskModel.transform(combinedDataset);
        return result;
    }

    public SDFGraph getSdfGraph() {
        return sdfGraph;
    }

    public void setSdfGraph(SDFGraph sdfGraph) {
        removeNode(getSdfGraph());
        this.sdfGraph = sdfGraph;
        addNode(sdfGraph);
    }

    public SGraph getDdfGraph() {
        return ddfGraph;
    }

    public void setDdfGraph(SGraph ddfGraph) {
        removeNode(getDdfGraph());
        this.ddfGraph = ddfGraph;
        addNode(getDdfGraph());
    }

    private Map<String, String> reverseMapKeyValue(Map<String, String> inputMap) {
        HashMap<String, String> reversedHashMap = new HashMap<>();
        for (String key : inputMap.keySet()) {
            reversedHashMap.put(inputMap.get(key), key);
        }
        return reversedHashMap;
    }

    private void setTrainingFlag(boolean flag) {
        ((InfusionStage) infusionNode.getSparkPipelineStage()).setTrainingFlag(flag);
    }

    private void setGoldenLinks(Dataset<Row> goldenLinks) {
        ((InfusionStage) infusionNode.getSparkPipelineStage()).setGoldenLinks(goldenLinks);
    }

    public String getSourceIdCol() {
        return SymbolTable.getInputSymbolValue(new Symbol(this, sourceIdCol));
    }

    public String getTargetIdCol() {
        return SymbolTable.getInputSymbolValue(new Symbol(this, targetIdCol));
    }
}
