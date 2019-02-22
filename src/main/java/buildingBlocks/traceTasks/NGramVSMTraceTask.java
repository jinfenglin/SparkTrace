package buildingBlocks.traceTasks;

import buildingBlocks.text2TFIDF.Text2NGramTFIDFPipeline;
import buildingBlocks.vecSimilarityPipeline.SparseCosinSimilarityPipeline;
import core.SparkTraceTask;
import core.graphPipeline.basic.SGraph;

/**
 *
 */
public class NGramVSMTraceTask implements TraceTaskBuilder {
    @Override
    public SGraph createSDF() throws Exception {
        SGraph graph = Text2NGramTFIDFPipeline.getGraph("NGramVSM_SDF");//text1,2 = "ngram-tf-idf1,2"
        graph.assignTypeToOutputField("ngram-tf-idf1", SGraph.SDFType.SOURCE_SDF);
        graph.assignTypeToOutputField("ngram-tf-idf2", SGraph.SDFType.TARGET_SDF);
        return graph;
    }

    @Override
    public SGraph createDDF() throws Exception {
        return SparseCosinSimilarityPipeline.getGraph("NGramVSM_DDF"); //vec1,2 - cosin_sim
    }

    @Override
    public SparkTraceTask connectTask(SparkTraceTask task) throws Exception {
        task.connect(task.sourceNode, "s_text", task.getSdfGraph(), "text1");
        task.connect(task.sourceNode, "t_text", task.getSdfGraph(), "text2");
        task.connect(task.getSdfGraph(), "ngram-tf-idf1", task.getDdfGraph(), "vec1");
        task.connect(task.getSdfGraph(), "ngram-tf-idf2", task.getDdfGraph(), "vec2");
        task.connect(task.getDdfGraph(), "cosin_sim", task.sinkNode, "ngram_vsm_sim");
        return task;
    }

    @Override
    public SparkTraceTask getTask(String sourceId, String targetId) throws Exception {
        SparkTraceTask task = new SparkTraceTask(createSDF(), createDDF(), sourceId, targetId);
        task.setVertexLabel("NGramVSM");
        task.addInputField("s_text").addInputField("t_text");
        task.addOutputField("ngram_vsm_sim");
        connectTask(task);
        return task;
    }
}
