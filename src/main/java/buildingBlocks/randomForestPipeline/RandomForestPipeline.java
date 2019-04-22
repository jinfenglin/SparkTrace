package buildingBlocks.randomForestPipeline;

import core.graphPipeline.basic.SGraph;
import core.graphPipeline.basic.SNode;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorIndexer;

public class RandomForestPipeline {
    public static String TRAIN_LABEL = "TRAIN_LABEL"; //INPUT
    public static String PREDICTION = "PREDICTION";//OUTPUT
    public static String FEATURES = "FEATURES", INDEXED_FEATURE = "INDEXED_FEATURE"; //INTERNAL

    public static SGraph getGraph(String graphName, String[] featureColSymbolName) throws Exception {
        SGraph graph = new SGraph(graphName);
        for (String featureColName : featureColSymbolName) {
            graph.addInputField(featureColName);
        }
        graph.addInputField(TRAIN_LABEL);
        graph.addOutputField(PREDICTION);


        VectorAssembler assembler = new VectorAssembler();
        SNode createFeatureVec = new SNode(assembler, "createFeatureVec");
        for (String symbolName : featureColSymbolName) {
            createFeatureVec.addInputField(symbolName);
        }
        createFeatureVec.addOutputField(FEATURES);

        VectorIndexer featureIndexer = new VectorIndexer().setMaxCategories(4);
        SNode fIndexNode = new SNode(featureIndexer, "FeatureIndex");
        fIndexNode.addInputField(FEATURES);
        fIndexNode.addOutputField(INDEXED_FEATURE);

        RandomForestClassifier rf = new RandomForestClassifier();
        rf.setNumTrees(100);
        rf.setMaxDepth(30);
        SNode rfNode = new SNode(rf, "RandomForest");
        rfNode.addInputField(TRAIN_LABEL);
        rfNode.addInputField(INDEXED_FEATURE);
        rfNode.addOutputField(PREDICTION);

        graph.addNode(createFeatureVec);
        graph.addNode(fIndexNode);
        graph.addNode(rfNode);

        for (String symbolName : featureColSymbolName) {
            graph.connect(graph.sourceNode, symbolName, createFeatureVec, symbolName);
        }
        graph.connect(createFeatureVec, FEATURES, fIndexNode, FEATURES);
        graph.connect(fIndexNode, INDEXED_FEATURE, rfNode, INDEXED_FEATURE);
        graph.connect(graph.sourceNode, TRAIN_LABEL, rfNode, TRAIN_LABEL);
        graph.connect(rfNode, PREDICTION, graph.sinkNode, PREDICTION);

        return graph;
    }
}
