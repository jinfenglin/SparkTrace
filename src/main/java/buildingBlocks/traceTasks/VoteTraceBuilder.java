package buildingBlocks.traceTasks;

import core.SparkTraceTask;
import core.graphPipeline.basic.SGraph;

/**
 *
 */
public class VoteTraceBuilder implements TraceTaskBuilder {
    @Override
    public SGraph createSDF() throws Exception {
        SGraph sdf = new SGraph();
        sdf.addInputField("s_text");
        sdf.addInputField("t_text");
        sdf.addOutputField("s_text_out");
        sdf.addOutputField("t_text_out");

        sdf.connect(sdf.sourceNode, "s_text", sdf.sinkNode, "s_text_out");
        sdf.connect(sdf.sourceNode, "t_text", sdf.sinkNode, "t_text_out");
        return sdf;
    }

    @Override
    public SGraph createDDF() throws Exception {
        SGraph ddf = new SGraph();
        ddf.addInputField("s_text");
        ddf.addInputField("t_text");
        ddf.addOutputField("vsm_out");
        ddf.addOutputField("lda_out");
        ddf.addOutputField("ngram_out");

        SparkTraceTask vsm = new VSMTraceBuilder().getTask("s_id", "t_id"); //s_text vsm_sim
        SparkTraceTask lda = new LDATraceBuilder().getTask("s_id", "t_id"); //s_text lda_sim
        SparkTraceTask ngram_vsm = new NGramVSMTraceTask().getTask("s_id", "t_id");//s_text ngram_vsm_sim

        ddf.addNode(vsm);
        ddf.addNode(lda);
        ddf.addNode(ngram_vsm);

        ddf.connect(ddf.sourceNode, "s_text", vsm, "s_text");
        ddf.connect(ddf.sourceNode, "s_text", lda, "s_text");
        ddf.connect(ddf.sourceNode, "s_text", ngram_vsm, "s_text");
        ddf.connect(ddf.sourceNode, "t_text", vsm, "t_text");
        ddf.connect(ddf.sourceNode, "t_text", lda, "t_text");
        ddf.connect(ddf.sourceNode, "t_text", ngram_vsm, "t_text");

        ddf.connect(vsm, "vsm_sim", ddf.sinkNode, "vsm_out");
        ddf.connect(lda, "lda_sim", ddf.sinkNode, "lda_out");
        ddf.connect(ngram_vsm, "ngram_vsm_sim", ddf.sinkNode, "ngram_out");
        return ddf;
    }

    @Override
    public SparkTraceTask connectTask(SparkTraceTask task) throws Exception {
        task.connect(task.sourceNode, "s_text", task.getSdfGraph(), "s_text");
        task.connect(task.sourceNode, "t_text", task.getSdfGraph(), "t_text");
        task.connect(task.getSdfGraph(), "s_text_out", task.getDdfGraph(), "s_text");
        task.connect(task.getSdfGraph(), "t_text_out", task.getDdfGraph(), "t_text");
        task.connect(task.getDdfGraph(), "vsm_out", task.sinkNode, "vsm_out");
        task.connect(task.getDdfGraph(), "lda_out", task.sinkNode, "lda_out");
        task.connect(task.getDdfGraph(), "ngram_out", task.sinkNode, "ngram_out");
        return task;
    }

    @Override
    public SparkTraceTask getTask(String sourceId, String targetId) throws Exception {
        SparkTraceTask task = new SparkTraceTask(createSDF(), createDDF(), sourceId, targetId);
        task.setId("vote");
        task.addInputField("s_text").addInputField("t_text");
        task.addOutputField("vsm_out");
        task.addOutputField("lda_out");
        task.addOutputField("ngram_out");
        connectTask(task);
        return task;
    }
}
