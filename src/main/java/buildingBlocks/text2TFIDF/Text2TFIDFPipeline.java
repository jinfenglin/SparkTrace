package buildingBlocks.text2TFIDF;

import buildingBlocks.preprocessor.EnglishPreprocess;
import buildingBlocks.vectorize.TFIDFPipeline;
import core.graphPipeline.basic.SGraph;

/**
 * Convert two columns of text into TF-IDF. Index the model on both column of text
 */
public class Text2TFIDFPipeline {
    public static SGraph getGraph(String graphName) throws Exception {
        SGraph graph = new SGraph(graphName);
        graph.addInputField("text1");
        graph.addInputField("text2");
        graph.addOutputField("tf-idf1");
        graph.addOutputField("tf-idf2");

        SGraph prep1 = EnglishPreprocess.getGraph("prep1");//text - cleanTokens
        SGraph prep2 = EnglishPreprocess.getGraph("prep2");
        SGraph tfidfPipe = TFIDFPipeline.getGraph("TFIDF"); //"tokens1,2"   - "tf-idf1,2"

        graph.addNode(prep1);
        graph.addNode(prep2);
        graph.addNode(tfidfPipe);//tokens1 tokens2 - tf-idf1 tf-idf2

        graph.connect(graph.sourceNode, "text1", prep1, "text");
        graph.connect(prep1, "cleanTokens", tfidfPipe, "tokens1");
        graph.connect(tfidfPipe, "tf-idf1", graph.sinkNode, "tf-idf1");

        graph.connect(graph.sourceNode, "text2", prep2, "text");
        graph.connect(prep2, "cleanTokens", tfidfPipe, "tokens2");
        graph.connect(tfidfPipe, "tf-idf2", graph.sinkNode, "tf-idf2");
        return graph;
    }
}
