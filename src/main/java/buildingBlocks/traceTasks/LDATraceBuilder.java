package buildingBlocks.traceTasks;

import buildingBlocks.text2TFIDF.Text2LDAPipeline;
import buildingBlocks.vecSimilarityPipeline.DenseCosinSimilarityPipeline;
import core.SparkTraceTask;
import core.graphPipeline.basic.SGraph;

/**
 *
 */
public class LDATraceBuilder implements TraceTaskBuilder {
    @Override
    public SGraph createSDF() throws Exception {
        SGraph graph = Text2LDAPipeline.getGraph("LDA_SDF");//text1,2 = "topics1,2"
        graph.assignTypeToOutputField("topics1", SGraph.SDFType.SOURCE_SDF);
        graph.assignTypeToOutputField("topics2", SGraph.SDFType.TARGET_SDF);
        return graph;
    }

    @Override
    public SGraph createDDF() throws Exception {
        return DenseCosinSimilarityPipeline.getGraph("LDA_DDF"); //vec1,2 - cosin_sim
    }

    @Override
    public SparkTraceTask connectTask(SparkTraceTask task) throws Exception {
        task.connect(task.sourceNode, "s_text", task.getSdfGraph(), "text1");
        task.connect(task.sourceNode, "t_text", task.getSdfGraph(), "text2");
        task.connect(task.getSdfGraph(), "topics1", task.getDdfGraph(), "vec1");
        task.connect(task.getSdfGraph(), "topics2", task.getDdfGraph(), "vec2");
        task.connect(task.getDdfGraph(), "cosin_sim", task.sinkNode, "lda_sim");
        return task;
    }

    @Override
    public SparkTraceTask getTask(String sourceId, String targetId) throws Exception {
        SparkTraceTask task = new SparkTraceTask(createSDF(), createDDF(), sourceId, targetId);
        task.setId("LDA");
        task.addInputField("s_text").addInputField("t_text");
        task.addOutputField("lda_sim");
        connectTask(task);
        return task;
    }
}
