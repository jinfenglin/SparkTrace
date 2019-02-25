package core;

import core.graphPipeline.basic.*;
import core.graphPipeline.graphSymbol.Symbol;
import core.graphPipeline.graphSymbol.SymbolTable;
import core.pipelineOptimizer.*;
import featurePipelineStages.LDAWithIO.LDAWithIO;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.clustering.LDAModel;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
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
    //Symbol name for id column which can be configured in different places.
    private String sourceIdCol, targetIdCol;

    private SGraph sourceSDF, targetSDF;
    private List<SGraph> unsupervisedLearnGraphs;
    private SGraph ddfGraph;

    private PipelineModel sourceSDFModel, targetSDFModel, ddfModel;
    private List<PipelineModel> unsupervisedModels;


    public SparkTraceTask(SGraph sourceSDF, SGraph targetSDF, List<SGraph> unsupervisedLearnGraphs, SGraph ddfGraph, String sourceIdCol, String targetIdCol) {
        super();
        this.sourceSDF = sourceSDF;
        this.targetSDF = targetSDF;
        this.unsupervisedLearnGraphs = unsupervisedLearnGraphs;
        unsupervisedModels = new ArrayList<>();
        this.ddfGraph = ddfGraph;
        addNode(sourceSDF);
        addNode(targetSDF);
        for (SGraph ug : unsupervisedLearnGraphs) {
            addNode(ug);
        }
        addNode(ddfGraph);

        this.sourceIdCol = sourceIdCol;
        this.targetIdCol = targetIdCol;
    }


    //
    private void mergeSubTask(SGraph parentSDF, SGraph childSDF, List<GraphHierarchyTree> path) throws Exception {
        SparkTraceTask childTask = (SparkTraceTask) childSDF.getContext();
        SGraph parentDDF = this.ddfGraph;
        SGraph childDDF = childTask.ddfGraph;
        parentSDF.addNode(childSDF);

        for (IOTableCell inputCell : childSDF.getInputTable().getCells()) {
            IOTableCell inputSource = inputCell.traceToSource(false, parentSDF);
            assert inputSource != inputCell; //SDF must have input
            childTask.disconnect(inputCell.getInputSource().get(0).getFieldSymbol(), inputCell.getFieldSymbol());//disconnect it from sourceNode
            parentSDF.connect(inputSource.getFieldSymbol(), inputCell.getFieldSymbol());
        }

        for (IOTableCell outputCell : childSDF.getOutputTable().getCells()) {
            List<IOTableCell> targetCells = outputCell.getOutputTarget();
            for (IOTableCell targetCell : new ArrayList<>(targetCells)) {
                String targetSymbolName = targetCell.getFieldSymbol().getSymbolName();
                String parentSDFNewOutputFieldName = targetSymbolName + "_" + UUID.randomUUID();
                String parentDDFNewInputFieldName = targetSymbolName + "_" + UUID.randomUUID();
                SGraph.SDFType type = childSDF.getOutputSymbolType(outputCell.getFieldSymbol().getSymbolName());
                parentSDF.addOutputField(parentSDFNewOutputFieldName, type);
                parentDDF.addInputField(parentDDFNewInputFieldName);
                parentSDF.connect(childSDF, outputCell.getFieldSymbol().getSymbolName(), parentSDF.sinkNode, parentSDFNewOutputFieldName); //connect childSDF to parentSDF
                connect(parentSDF, parentSDFNewOutputFieldName, parentDDF, parentDDFNewInputFieldName);//connect new added field from parentSDF to parentDDF
                int pathIndex = path.size() - 1;
                if (path.size() > 2) {
                    pathIndex -= 1; //remove the graph where parentDDF reside (this task) and take parentDDF as context graph (because subtask must in parentDDF)
                    SGraph contextGraph = path.get(pathIndex).getNodeContent();
                    String penetrationOutputFiledName = parentDDFNewInputFieldName;
                    while (pathIndex > 0) {
                        //Connect the new added output to inner graph's added input field
                        String penetrationInputFiledName = targetCell.getFieldSymbol().getSymbolName() + "_" + UUID.randomUUID();
                        pathIndex -= 1;
                        SGraph innerSGraph = path.get(pathIndex).getNodeContent();
                        innerSGraph.addInputField(penetrationInputFiledName);
                        contextGraph.connect(contextGraph.sourceNode, penetrationOutputFiledName, innerSGraph, penetrationInputFiledName);
                        contextGraph = innerSGraph;
                        penetrationOutputFiledName = penetrationInputFiledName;
                    }
                    contextGraph.connect(contextGraph.sourceNode, penetrationOutputFiledName, childDDF, targetCell.getFieldSymbol().getSymbolName());
                    childTask.disconnect(childSDF, outputCell.getFieldSymbol().getSymbolName(), childDDF, targetCell.getFieldSymbol().getSymbolName());
                }
            }
        }
        childTask.removeNodeWithoutCleanRelations(childSDF);
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
                    GraphHierarchyTree subTaskDDFTreeNode = ght.findNode(subTask.ddfGraph);
                    GraphHierarchyTree parentDDFTreeNode = ght.findNode(ddfGraph);
                    List<GraphHierarchyTree> path = new ArrayList<>();
                    ght.findPath(parentDDFTreeNode, subTaskDDFTreeNode, path);
                    //Merge the childSTT to the parent STT
                    mergeSubTask(getSourceSDFSdfGraph(), subTask.getSourceSDFSdfGraph(), path);
                    mergeSubTask(getTargetSDFSdfGraph(), subTask.getTargetSDFSdfGraph(), path);
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
        PipelineModel sourceSDFModel = this.getSourceSDFSdfGraph().toPipeline().fit(sourceArtifacts);
        PipelineModel targetSDFModel = this.getTargetSDFSdfGraph().toPipeline().fit(targetArtifacts);
        this.sourceSDFModel = sourceSDFModel;
        this.targetSDFModel = targetSDFModel;
        Dataset<Row> sourceSDFeatureVecs = sourceSDFModel.transform(sourceArtifacts);
        Dataset<Row> targetSDFeatureVecs = targetSDFModel.transform(targetArtifacts);

        unsupervisedLeanring(sourceSDFeatureVecs, targetSDFeatureVecs);
        int i = 0;
        for (SGraph unsupervisedLearnGraph : this.unsupervisedLearnGraphs) {
            PipelineModel unsupervisedModel = this.unsupervisedModels.get(i);
            String inputColParam = "inputCol";
            String outputColParam = "outputCol";
            if (unsupervisedModel.stages()[0] instanceof LDAModel) {
                inputColParam = "featuresCol";
                outputColParam = "topicDistributionCol";
            }
            unsupervisedModel.stages()[0].set(inputColParam, unsupervisedLearnGraph.getInputTable().getCells().get(0).getFieldSymbol().getSymbolValue());
            unsupervisedModel.stages()[0].set(outputColParam, unsupervisedLearnGraph.getOutputTable().getCells().get(0).getFieldSymbol().getSymbolValue());
            sourceSDFeatureVecs = unsupervisedModel.transform(sourceSDFeatureVecs);

            unsupervisedModel.stages()[0].set(inputColParam, unsupervisedLearnGraph.getInputTable().getCells().get(1).getFieldSymbol().getSymbolValue());
            unsupervisedModel.stages()[0].set(outputColParam, unsupervisedLearnGraph.getOutputTable().getCells().get(1).getFieldSymbol().getSymbolValue());
            targetSDFeatureVecs = unsupervisedModel.transform(targetSDFeatureVecs);
            i++;
        }


        Dataset<Row> candidateLinks;
        if (goldenLinks == null) {
            candidateLinks = sourceSDFeatureVecs.crossJoin(targetSDFeatureVecs); //Cross join
        } else {
            candidateLinks = appendFeaturesToLinks(goldenLinks.toDF(), sourceSDFeatureVecs, targetSDFeatureVecs);
        }

        PipelineModel ddfModel = ddfGraph.toPipeline().fit(candidateLinks);
        this.ddfModel = ddfModel;
    }

    private void unsupervisedLeanring(Dataset<Row> sourceSDFeatureVecs, Dataset<Row> targetSDFFeatreusVecs) throws Exception {
        for (SGraph unsupervisedLearnGraph : this.unsupervisedLearnGraphs) {
            Set<String> sourceColNames = new HashSet<>(Arrays.asList(sourceSDFeatureVecs.columns()));
            Set<String> targetColNames = new HashSet<>(Arrays.asList(targetSDFFeatreusVecs.columns()));
            String mixedInputCol = "tmpMixedCol";
            String fieldName = unsupervisedLearnGraph.getInputTable().getCells().get(0).getFieldSymbol().getSymbolValue();
            DataType columnDataType = null;
            for (StructField sf : sourceSDFeatureVecs.schema().fields()) {
                if (sf.name().equals(fieldName)) {
                    columnDataType = sf.dataType();
                }
            }
            StructField field = DataTypes.createStructField(mixedInputCol, columnDataType, false);
            StructType st = new StructType(new StructField[]{field});
            Dataset<Row> trainingData = sourceSDFeatureVecs.sparkSession().createDataFrame(new ArrayList<>(), st);

            for (IOTableCell inputCell : unsupervisedLearnGraph.getInputTable().getCells()) {
                String fieldValue = inputCell.getFieldSymbol().getSymbolValue();
                Dataset<Row> columnData = null;
                if (sourceColNames.contains(fieldValue)) {
                    columnData = sourceSDFeatureVecs.select(fieldValue);
                } else if (targetColNames.contains(fieldValue)) {
                    columnData = targetSDFFeatreusVecs.select(fieldValue);
                }
                trainingData = trainingData.union(columnData);
                //TODO: currently assume only one stage inside. This should be modified
            }
            Pipeline unsupervisePipe = unsupervisedLearnGraph.toPipeline();
            PipelineStage innerStage = unsupervisePipe.getStages()[0];
            String inputColParam = "inputCol";
            if (innerStage instanceof LDAWithIO) {
                inputColParam = "featuresCol";
            }
            innerStage.set(inputColParam, mixedInputCol);
            unsupervisePipe.setStages(new PipelineStage[]{innerStage});
            this.unsupervisedModels.add(unsupervisePipe.fit(trainingData));
        }
    }

    private Dataset<Row> appendFeaturesToLinks(Dataset<Row> links, Dataset<Row> sourceFeatures, Dataset<Row> targetFeatures) {
        String sourceIDColName = getSourceIdCol();
        String targetIDColName = getTargetIdCol();
        Column sourceArtifactIdCol = sourceFeatures.col(sourceIDColName);
        Column targetArtifactIdCol = targetFeatures.col(targetIDColName);

        Column linkSourceIdCol = links.col(sourceIDColName);
        Column linkTargetIdCol = links.col(targetIDColName);

        Dataset<Row> linksWithFeatureVec = links.join(sourceFeatures, sourceArtifactIdCol.equalTo(linkSourceIdCol));
        linksWithFeatureVec = linksWithFeatureVec.join(targetFeatures, targetArtifactIdCol.equalTo(linkTargetIdCol));
        linksWithFeatureVec = linksWithFeatureVec.drop(sourceArtifactIdCol).drop(targetArtifactIdCol);
        return linksWithFeatureVec;
    }

    public Dataset<Row> trace(Dataset<? extends TraceArtifact> sourceArtifacts,
                              Dataset<? extends TraceArtifact> targetArtifacts) {
        Dataset<Row> sourceSDFeatureVecs = sourceSDFModel.transform(sourceArtifacts);
        Dataset<Row> targetSDFeatureVecs = targetSDFModel.transform(targetArtifacts);

        int i = 0;
        for (PipelineModel unsupervisedModel : this.unsupervisedModels) {
            SGraph unsupervisedLearnGraph = this.unsupervisedLearnGraphs.get(i);
            String inputColParam = "inputCol";
            String outputColParam = "outputCol";
            if (unsupervisedModel.stages()[0] instanceof LDAModel) {
                inputColParam = "featuresCol";
                outputColParam = "topicDistributionCol";
            }
            unsupervisedModel.stages()[0].set(inputColParam, unsupervisedLearnGraph.getInputTable().getCells().get(0).getFieldSymbol().getSymbolValue());
            unsupervisedModel.stages()[0].set(outputColParam, unsupervisedLearnGraph.getOutputTable().getCells().get(0).getFieldSymbol().getSymbolValue());
            sourceSDFeatureVecs = unsupervisedModel.transform(sourceSDFeatureVecs);
            unsupervisedModel.stages()[0].set(inputColParam, unsupervisedLearnGraph.getInputTable().getCells().get(1).getFieldSymbol().getSymbolValue());
            unsupervisedModel.stages()[0].set(outputColParam, unsupervisedLearnGraph.getOutputTable().getCells().get(1).getFieldSymbol().getSymbolValue());
            targetSDFeatureVecs = unsupervisedModel.transform(targetSDFeatureVecs);
            i++;
        }
        Dataset<Row> candidateLinks = sourceSDFeatureVecs.crossJoin(targetSDFeatureVecs); //Cross join
        return this.ddfModel.transform(candidateLinks);
    }

    public SGraph getSourceSDFSdfGraph() {
        return sourceSDF;
    }

    public SGraph getTargetSDFSdfGraph() {
        return targetSDF;
    }


    public List<SGraph> getUnsupervisedLearnGraph() {
        return unsupervisedLearnGraphs;
    }


    public SGraph getDdfGraph() {
        return ddfGraph;
    }

    public void setDdfGraph(SGraph ddfGraph) throws Exception {
        removeNode(getDdfGraph());
        this.ddfGraph = ddfGraph;
        addNode(getDdfGraph());
    }

    public String getSourceIdCol() {
        return SymbolTable.getSymbolValue(new Symbol(this, sourceIdCol));
    }

    public String getTargetIdCol() {
        return SymbolTable.getSymbolValue(new Symbol(this, targetIdCol));
    }
}
