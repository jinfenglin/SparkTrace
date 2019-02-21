package buildingBlocks.preprocessor;

import core.graphPipeline.basic.SGraph;
import core.graphPipeline.basic.SNode;
import featurePipelineStages.NullRemoveWrapper.NullRemoverModelSingleIO;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.mllib.feature.Stemmer;

/**
 * A pre processing pipeline for english document. Tokenize, remove stop words, stem
 */
public class EnglishPreprocess {
    public static SGraph getGraph(String graphName) throws Exception {
        SGraph graph = new SGraph(graphName);
        graph.addInputField("text");
        graph.addOutputField("cleanTokens");

        Tokenizer tk = new Tokenizer();
        SNode tkNode = new SNode(new NullRemoverModelSingleIO(tk), "tokenizer");
        tkNode.addInputField("text");
        tkNode.addOutputField("tokens");

        StopWordsRemover remover = new StopWordsRemover();
        SNode removerNode = new SNode(new NullRemoverModelSingleIO(remover), "stopWordRemover");
        removerNode.addInputField("tokens");
        removerNode.addOutputField("cleanTokens");

//        Stemmer stemmer = new Stemmer();
//        SNode stemmerNode = new SNode(new NullRemoverModelSingleIO(stemmer), "stemmerNode");
//        stemmerNode.addInputField("cleanTokens");
//        stemmerNode.addOutputField("stemTokens");

        graph.addNode(tkNode);
        graph.addNode(removerNode);
//        graph.addNode(stemmerNode);

        graph.connect(graph.sourceNode, "text", tkNode, "text");
        graph.connect(tkNode, "tokens", removerNode, "tokens");
//        graph.connect(removerNode, "cleanTokens", stemmerNode, "cleanTokens");
//        graph.connect(stemmerNode, "stemTokens", graph.sinkNode, "cleanTokens");
        graph.connect(removerNode, "cleanTokens", graph.sinkNode, "cleanTokens");
        return graph;
    }
}
