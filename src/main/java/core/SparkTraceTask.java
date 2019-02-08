package core;

import core.graphPipeline.SDF.SDFGraph;
import core.graphPipeline.basic.IOTableCell;
import core.graphPipeline.basic.SGraph;
import core.graphPipeline.basic.Vertex;
import core.graphPipeline.graphSymbol.Symbol;
import core.graphPipeline.graphSymbol.SymbolTable;
import core.pipelineOptimizer.*;
import javafx.util.Pair;
import org.apache.spark.SparkContext;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.util.SchemaUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import traceability.components.abstractComponents.TraceArtifact;
import traceability.components.abstractComponents.TraceLink;

import java.util.*;

import static core.pipelineOptimizer.PipelineOptimizer.createUniqueNewFieldName;
import static org.apache.spark.sql.functions.col;
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
    private SparkSession sparkSession;
    private SDFGraph sdfGraph;
    private SGraph ddfGraph;

    private PipelineModel SDFModel;
    private PipelineModel DDFModel;


    public SparkTraceTask(SparkSession sparkSession) {
        super();
        this.sparkSession = sparkSession;
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
        parentSDF.addNode(childSDF); //Move the childSDF to parent SDF

        //The input of childSTT are from parent SDF and passed to childSTT through the ouputfiled of parent SDF.
        //Once the childSDF is moved into parent SDF, the input provider should connect the childSDF directly


        //connect the childSDF to childDDF
        for (IOTableCell outputCell : childSDF.getOutputTable().getCells()) {
            String addedOutputFieldName = createUniqueNewFieldName(outputCell);
            Symbol newParentOutputFiled = new Symbol(parentSDF, addedOutputFieldName);
            parentSDF.addOutputField(newParentOutputFiled);
            parentSDF.connect(childSDF, outputCell.getFieldSymbol().getSymbolName(), parentSDF.sinkNode, addedOutputFieldName);
            addedOutputFields.add(newParentOutputFiled);
        }

        //TODO fix this
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

                List<IOTableCell> subTaskSDFIOCells = innerTask.getSdfGraph().getOutputTable().getCells();
                //The innerSDF keeps the connection to inner DDF, use this connection to connect innerSTT's sourceNode and innerDDF
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
                    innerTask.connect(innerTask.sourceNode, innerSTTInputFiledSymbolName, innerTask.getDdfGraph(), innerDDFInputFiledSymbolName);
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

    private Dataset<Row> getSourceSDFFeatureVecs(Dataset<Row> mixedSDFeatureVecs) {
        Set<String> sourceSDFCols = sdfGraph.getSourceSDFOutput();
        String sourceIdColName = sdfGraph.getSourceIdCol();
        sourceSDFCols.add(sourceIdColName);
        Seq<String> sourceFeatureCols = JavaConverters.asScalaIteratorConverter(sourceSDFCols.iterator()).asScala().toSeq();
        return mixedSDFeatureVecs.selectExpr(sourceFeatureCols).where(col(sourceIdColName).isNotNull());
    }

    private Dataset<Row> getTargetSDFFeatureVecs(Dataset<Row> mixedSDFeatureVecs) {
        Set<String> targetSDFCols = sdfGraph.getTargetSDFOutputs();
        String targetIdColName = sdfGraph.getTargetIdCol();
        targetSDFCols.add(targetIdColName);
        Seq<String> targetFeatureCols = JavaConverters.asScalaIteratorConverter(targetSDFCols.iterator()).asScala().toSeq();
        return mixedSDFeatureVecs.selectExpr(targetFeatureCols).where(col(targetIdColName).isNotNull());
    }


    /**
     * Execute this STT as a top level STT.
     */
    public void train(Dataset<? extends TraceArtifact> sourceArtifacts,
                      Dataset<? extends TraceArtifact> targetArtifacts,
                      Dataset<? extends TraceLink> goldenLinks) throws Exception {
        Dataset<Row> combinedDataset = UnionSourceAndTarget(sourceArtifacts, targetArtifacts);
        SDFModel = sdfGraph.toPipeline().fit(combinedDataset);

        // Synchronize the symbol value for SDFGraph and DDFGraph. SDFGraph output -> DDFGraph inputs
        for (IOTableCell providerCell : sdfGraph.getOutputTable().getCells()) {
            Symbol providerSymbol = providerCell.getFieldSymbol();
            for (IOTableCell receiverCell : providerCell.getOutputTarget()) {
                Symbol receiverSymbol = receiverCell.getFieldSymbol();
                SymbolTable.shareSymbolValue(providerSymbol, receiverSymbol, true);
            }
        }

        Dataset<Row> mixedSDFeatureVecs = SDFModel.transform(combinedDataset);
        Dataset<Row> sourceSDFeatureVecs = getSourceSDFFeatureVecs(mixedSDFeatureVecs);
        Dataset<Row> targetSDFeatureVecs = getTargetSDFFeatureVecs(mixedSDFeatureVecs);
        Dataset<Row> goldLinksWithFeatureVec = null;
        if (goldenLinks == null) {
            List<StructField> fields = new ArrayList<>();
            for (StructField sourceField : sourceSDFeatureVecs.schema().fields()) {
                fields.add(sourceField);
            }
            for (StructField targetField : targetSDFeatureVecs.schema().fields()) {
                fields.add(targetField);
            }
            StructType emptyDFSchema = new StructType(fields.toArray(new StructField[0]));
            goldLinksWithFeatureVec = sparkSession.createDataFrame(new ArrayList<>(), emptyDFSchema);
        } else {
            goldLinksWithFeatureVec = appendFeaturesToLinks(goldenLinks.toDF(), sourceSDFeatureVecs, targetSDFeatureVecs);
        }
        DDFModel = ddfGraph.toPipeline().fit(goldLinksWithFeatureVec);
    }

    public Dataset<Row> trace(Dataset<? extends TraceArtifact> sourceArtifacts,
                              Dataset<? extends TraceArtifact> targetArtifacts) {
        Dataset<Row> combinedDataset = UnionSourceAndTarget(sourceArtifacts, targetArtifacts);
        Dataset<Row> mixedSDFeatureVecs = SDFModel.transform(combinedDataset);
        Dataset<Row> sourceSDFeatureVecs = getSourceSDFFeatureVecs(mixedSDFeatureVecs);
        Dataset<Row> targetSDFeatureVecs = getTargetSDFFeatureVecs(mixedSDFeatureVecs);

        Dataset<Row> candidateLinks = sourceSDFeatureVecs.crossJoin(targetSDFeatureVecs); //Cross join
        Dataset<Row> traceResult = DDFModel.transform(candidateLinks);
        return traceResult;
    }


    private Dataset<Row> appendFeaturesToLinks(Dataset<Row> links, Dataset<Row> sourceFeatures, Dataset<Row> targetFeatures) {
        String sourceIDColName = sdfGraph.getSourceIdCol();
        String targetIDColName = sdfGraph.getTargetIdCol();
        Column sourceArtifactIdCol = sourceFeatures.col(sourceIDColName);
        Column targetArtifactIdCol = targetFeatures.col(targetIDColName);

        Column linkSourceIdCol = links.col(sourceIDColName);
        Column linkTargetIdCol = links.col(targetIDColName);

        Dataset<Row> linksWithFeatureVec = links.join(sourceFeatures, sourceArtifactIdCol.equalTo(linkSourceIdCol));
        linksWithFeatureVec = linksWithFeatureVec.join(targetFeatures, targetArtifactIdCol.equalTo(linkTargetIdCol));
        linksWithFeatureVec = linksWithFeatureVec.drop(sourceArtifactIdCol).drop(targetArtifactIdCol);
        return linksWithFeatureVec;
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
}
