package core;

import core.GraphSymbol.Symbol;
import core.pipelineOptimizer.*;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.util.SchemaUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.spark_project.dmg.pmml.OutputField;
import traceability.components.abstractComponents.TraceArtifact;
import traceability.components.abstractComponents.TraceLink;

import java.util.*;


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

    private SDFGraph sourceSDFGraph, targetSDFGraph;
    private SGraph DDFGraph;

    private PipelineModel sourceSDFModel, targetSDFModel;
    private PipelineModel DDFModel;


    public SparkTraceTask() {
        super();
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
            this.connect(SDF, fromSymbolName, DDF, toSymbolName);//TODO toSymbolName should be unique name
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
        parentSDF.addNode(childSDF);
        for (IOTableCell outputCell : childSDF.getOutputTable().getCells()) {
            //Creating symbol in parent with child's symbol name may lead to name conflict, should create a unique name here
            //These intermediate symbol name will not impact the output name because the symbol value is passed from child SDF
            // TODO: Use unique name here to avoid name conflict
            String addedOutputFieldName = outputCell.getFieldSymbol().getSymbolName();
            Symbol newParentOutputFiled = new Symbol(parentSDF, addedOutputFieldName);
            parentSDF.addOutputField(newParentOutputFiled);
            parentSDF.connect(childSDF, addedOutputFieldName, parentSDF.sinkNode, addedOutputFieldName);
            addedOutputFields.add(newParentOutputFiled);
        }

        for (IOTableCell inputCell : childSDF.getInputTable().getCells()) {
            String addedInputFieldName = inputCell.getFieldSymbol().getSymbolName(); //This name should not change
            Symbol newParentInputField = new Symbol(parentSDF, addedInputFieldName);
            parentSDF.addInputField(newParentInputField);
            parentSDF.connect(parentSDF.sourceNode, addedInputFieldName, childSDF, addedInputFieldName);
        }

        SGraph lastGraph = path.get(0).getNodeContent();
        List<String> addedSymbolNames = connectSDFToDDF(addedOutputFields, parentSDF, lastGraph, SDFSymbolMap);
        for (int i = 1; i < path.size(); i++) {
            SGraph curGraph = path.get(i).getNodeContent();
            if (curGraph instanceof SparkTraceTask) {
                //if current level is a STT connect the STT input to its DDF input
                SparkTraceTask innerTask = (SparkTraceTask) curGraph;
                Map<String, String> reversedSDFSymbolMap = reverseMapKeyValue(SDFSymbolMap);

                List<IOTableCell> subTaskSDFIOCells = null;
                if (parentSDF.equals(sourceSDFGraph)) {
                    subTaskSDFIOCells = innerTask.getSourceSDFGraph().getOutputTable().getCells();
                } else {
                    subTaskSDFIOCells = innerTask.getTargetSDFGraph().getOutputTable().getCells();
                }

                //The innerSDF keeps the connection to inner DDF, use this connection to connect innerSTT's sourceNode and innerDDF
                for (IOTableCell cell : subTaskSDFIOCells) {
                    List<IOTableCell> connectedCells = cell.getOutputTarget();
                    IOTableCell parentSDFOutputCell = null;
                    IOTableCell innerTaskDDFInputCell = null;
                    for (IOTableCell conCell : connectedCells) {
                        if (conCell.getParentTable().getContext().equals(parentSDF.sinkNode)) {
                            parentSDFOutputCell = conCell;
                        } else if (conCell.getParentTable().getContext().equals(innerTask.getDDFGraph().sourceNode)) {
                            innerTaskDDFInputCell = conCell;
                        }
                    }
                    String innerSTTInputFiledSymbolName = reversedSDFSymbolMap.get(parentSDFOutputCell.getFieldSymbol().getSymbolName());
                    String innerDDFInputFiledSymbolName = innerTaskDDFInputCell.getFieldSymbol().getSymbolName();
                    innerTask.connect(innerTask.sourceNode, innerSTTInputFiledSymbolName, innerTask.getDDFGraph(), innerDDFInputFiledSymbolName);
                }
            } else {
                //Create input field for current graph, connect the input from parent graph to current graph
                for (String symbolName : addedSymbolNames) {
                    String addInputFieldName = symbolName; //TODO unique name
                    String symbolNameInSDF = SDFSymbolMap.get(symbolName);
                    SDFSymbolMap.remove(symbolName);
                    SDFSymbolMap.put(addInputFieldName, symbolNameInSDF);
                    curGraph.addInputField(addInputFieldName);
                    lastGraph.connect(lastGraph.sourceNode, symbolName, curGraph, addInputFieldName);
                }
            }
            lastGraph = curGraph;
        }
    }

    /**
     * Collect SDFs from child STTs by scanning the nodes with BFS. If a STT found then register it; if
     * a Graph found, add all its nodes to search list; If a node found ignore it.
     * Take O(n) time.
     */
    public void initSTT() throws Exception {
        GraphHierarchyTree ght = new GraphHierarchyTree(null, this);
        if (!isInitialed) {
            isInitialed = true;
            List<Vertex> nodes = DDFGraph.getNodes();
            for (Vertex node : nodes) {
                if (node instanceof SparkTraceTask) {
                    SparkTraceTask subTask = (SparkTraceTask) node;
                    //Make sure the child STT have merged all inner STTs
                    subTask.initSTT();

                    //Find a path from parent STT's DDF graph to subTask in GHT, the first node in path is parent DDF
                    GraphHierarchyTree sparkTaskTreeNode = ght.findNode(subTask);
                    GraphHierarchyTree DDFTreeNode = ght.findNode(DDFGraph);
                    List<GraphHierarchyTree> path = new ArrayList<>();
                    ght.findPath(DDFTreeNode, sparkTaskTreeNode, path);

                    //Merge the childSTT to the parent STT
                    mergeSDF(sourceSDFGraph, subTask.getSourceSDFGraph(), path);
                    mergeSDF(targetSDFGraph, subTask.getTargetSDFGraph(), path);
                }
            }
        }
    }

    /**
     * Execute this STT as a top level STT.
     */
    public void train(Dataset<? extends TraceArtifact> sourceArtifacts,
                      Dataset<? extends TraceArtifact> targetArtifacts,
                      Dataset<? extends TraceLink> goldenLinks) throws Exception {
        sourceSDFModel = sourceSDFGraph.toPipeline().fit(sourceArtifacts);
        targetSDFModel = targetSDFGraph.toPipeline().fit(targetArtifacts);
        Dataset<Row> sourceSDFeatureVecs = sourceSDFModel.transform(sourceArtifacts);
        Dataset<Row> targetSDFeatureVecs = targetSDFModel.transform(targetArtifacts);
        if (!(goldenLinks == null)) {
            Dataset<Row> goldLinksWithFeatureVec = appendFeaturesToLinks(goldenLinks.toDF(), sourceSDFeatureVecs, targetSDFeatureVecs);
            DDFModel = DDFGraph.toPipeline().fit(goldLinksWithFeatureVec);
            Dataset<Row> traceResult = DDFModel.transform(goldLinksWithFeatureVec);
            traceResult.show();
        }
    }

    public void trace(Dataset<? extends TraceArtifact> sourceArtifacts,
                      Dataset<? extends TraceArtifact> targetArtifacts) {
        Dataset<Row> sourceFeatures = sourceSDFModel.transform(sourceArtifacts);
        Dataset<Row> targetFeatures = targetSDFModel.transform(targetArtifacts);

        Dataset<Row> candidateLinks = sourceArtifacts.crossJoin(targetArtifacts); //Cross join
        candidateLinks = appendFeaturesToLinks(candidateLinks, sourceFeatures, targetFeatures);
        Dataset<Row> traceResult = DDFModel.transform(candidateLinks);
    }


    private Dataset<Row> appendFeaturesToLinks(Dataset<Row> links, Dataset<Row> sourceFeatures, Dataset<Row> targetFeatures) {
        String sourceIDColName = sourceSDFGraph.getArtifactIdColName();
        String targetIDColName = targetSDFGraph.getArtifactIdColName();
        Column sourceArtifactIdCol = sourceFeatures.col(sourceIDColName);
        Column targetArtifactIdCol = targetFeatures.col(targetIDColName);

        Column linkSourceIdCol = links.col(sourceIDColName);
        Column linkTargetIdCol = links.col(targetIDColName);

        Dataset<Row> linksWithFeatureVec = links.join(sourceFeatures, sourceArtifactIdCol.equalTo(linkSourceIdCol));
        linksWithFeatureVec = linksWithFeatureVec.join(targetFeatures, targetArtifactIdCol.equalTo(linkTargetIdCol));
        linksWithFeatureVec = linksWithFeatureVec.drop(sourceArtifactIdCol).drop(targetArtifactIdCol);
        return linksWithFeatureVec;
    }


    public SGraph getSourceSDFGraph() {
        return sourceSDFGraph;
    }

    public void setSourceSDFGraph(SDFGraph sourceSDFGraph) {
        removeNode(getSourceSDFGraph());
        this.sourceSDFGraph = sourceSDFGraph;
        addNode(sourceSDFGraph);
    }

    public SGraph getTargetSDFGraph() {
        return targetSDFGraph;
    }

    public void setTargetSDFGraph(SDFGraph targetSDFGraph) {
        removeNode(getTargetSDFGraph());
        this.targetSDFGraph = targetSDFGraph;
        addNode(targetSDFGraph);
    }

    public SGraph getDDFGraph() {
        return DDFGraph;
    }

    public void setDDFGraph(SGraph DDFGraph) {
        removeNode(getDDFGraph());
        this.DDFGraph = DDFGraph;
        addNode(getDDFGraph());
    }

    private Map<String, String> reverseMapKeyValue(Map<String, String> inputMap) {
        HashMap<String, String> reversedHashMap = new HashMap<>();
        for (String key : inputMap.keySet()) {
            reversedHashMap.put(inputMap.get(key), key);
        }
        return reversedHashMap;
    }
}
