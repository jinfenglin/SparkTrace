package buildingBlocks.preprocessor;

import core.graphPipeline.basic.SGraph;
import core.graphPipeline.basic.SNode;
import featurePipelineStages.NullRemoveWrapper.NullRemoverModelSingleIO;
import org.apache.spark.ml.feature.NGram;

/**
 *
 */
public class NGramPreprocessPipeline {
    public static SGraph getGraph(String graphName) throws Exception {
        SGraph graph = new SGraph(graphName);
        graph.addInputField("text");
        graph.addOutputField("ngram_tokens");

        SGraph prep1 = EnglishPreprocess.getGraph("preprocess");//text - cleanTokens

        NGram ngramStage = new NGram().setN(2);
        SNode ngramNode = new SNode(new NullRemoverModelSingleIO(ngramStage), "ngram");
        ngramNode.addInputField("tokens");
        ngramNode.addOutputField("ngrams");

        graph.addNode(prep1);
        graph.addNode(ngramNode);

        graph.connect(graph.sourceNode, "text", prep1, "text");
        graph.connect(prep1, "cleanTokens", ngramNode, "tokens");
        graph.connect(ngramNode, "ngrams", graph.sinkNode, "ngram_tokens");

        return graph;
    }
}
