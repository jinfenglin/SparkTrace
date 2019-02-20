package buildingBlocks.text2TFIDF;

import buildingBlocks.preprocessor.NGramPreprocessPipeline;
import buildingBlocks.vectorize.TFIDFPipeline;
import core.graphPipeline.basic.SGraph;

/**
 *
 */
public class Text2NGramTFIDFPipeline {
    public static SGraph getGraph(String graphName) throws Exception {
        SGraph graph = new SGraph(graphName);
        graph.addInputField("text1");
        graph.addInputField("text2");
        graph.addOutputField("ngram-tf-idf1");
        graph.addOutputField("ngram-tf-idf2");

        SGraph ngramPreprocessPipe1 = NGramPreprocessPipeline.getGraph("ngram1"); // text - ngram_tokens
        SGraph ngramPreprocessPipe2 = NGramPreprocessPipeline.getGraph("ngram2"); // text - ngram_tokens
        SGraph tfidfPipe = TFIDFPipeline.getGraph("tf-idfPipe"); //tokens1 - "tf-idf1"

        graph.addNode(ngramPreprocessPipe1);
        graph.addNode(ngramPreprocessPipe2);
        graph.addNode(tfidfPipe);

        graph.connect(graph.sourceNode, "text1", ngramPreprocessPipe1, "text");
        graph.connect(graph.sourceNode, "text2", ngramPreprocessPipe2, "text");
        graph.connect(ngramPreprocessPipe1, "ngram_tokens", tfidfPipe, "tokens1");
        graph.connect(ngramPreprocessPipe2, "ngram_tokens", tfidfPipe, "tokens2");
        graph.connect(tfidfPipe, "tf-idf1", graph.sinkNode, "ngram-tf-idf1");
        graph.connect(tfidfPipe, "tf-idf2", graph.sinkNode, "ngram-tf-idf2");
        return graph;
    }

}
