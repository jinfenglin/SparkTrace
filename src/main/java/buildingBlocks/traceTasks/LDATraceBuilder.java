package buildingBlocks.traceTasks;

import buildingBlocks.preprocessor.SimpleWordCount;
import buildingBlocks.unsupervisedLearn.LDAGraphPipeline;
import buildingBlocks.vecSimilarityPipeline.DenseCosinSimilarityPipeline;
import core.SparkTraceTask;
import core.graphPipeline.basic.SGraph;

import java.util.Arrays;

public class LDATraceBuilder implements TraceTaskBuilder {
    public static String INPUT1 = "s_text", INPUT2 = "t_text";
    public static String OUTPUT = "lda_sim";

    public SGraph createSSDF() throws Exception {
        SGraph graph = SimpleWordCount.getGraph("LDA_SSDF");//text1,2 = "topics1,2"
        return graph;
    }


    public SGraph createTSDF() throws Exception {
        SGraph graph = SimpleWordCount.getGraph("LDA_TSDF");//text1,2 = "topics1,2"
        return graph;
    }

    @Override
    public SGraph createSDF() throws Exception {
        return null;
    }

    @Override
    public SGraph createDDF() throws Exception {
        return DenseCosinSimilarityPipeline.getGraph("LDA_DDF"); //vec1,2 - cosin_sim
    }

    @Override
    public SparkTraceTask connectTask(SparkTraceTask task) throws Exception {
        task.connect(task.sourceNode, INPUT1, task.getSourceSDFSdfGraph(), SimpleWordCount.INPUT_TEXT_COL);
        task.connect(task.sourceNode, INPUT2, task.getTargetSDFSdfGraph(), SimpleWordCount.INPUT_TEXT_COL);
        task.connect(task.getSourceSDFSdfGraph(), SimpleWordCount.OUTPUT_HTF, task.getUnsupervisedLearnGraph().get(0), LDAGraphPipeline.INPUT1);
        task.connect(task.getTargetSDFSdfGraph(), SimpleWordCount.OUTPUT_HTF, task.getUnsupervisedLearnGraph().get(0), LDAGraphPipeline.INPUT2);
        task.connect(task.getUnsupervisedLearnGraph().get(0), LDAGraphPipeline.OUTPUT1, task.getDdfGraph(), DenseCosinSimilarityPipeline.INPUT1);
        task.connect(task.getUnsupervisedLearnGraph().get(0), LDAGraphPipeline.OUTPUT2, task.getDdfGraph(), DenseCosinSimilarityPipeline.INPUT2);
        task.connect(task.getDdfGraph(), DenseCosinSimilarityPipeline.OUTPUT, task.sinkNode, OUTPUT);
        return task;
    }

    public SGraph createUnsupervise() throws Exception {
        return LDAGraphPipeline.getGraph("SharedLDA");
    }

    @Override
    public SparkTraceTask getTask(String sourceId, String targetId) throws Exception {
        SparkTraceTask task = new SparkTraceTask(createSSDF(), createTSDF(), Arrays.asList(createUnsupervise()), createDDF(), sourceId, targetId);
        task.setVertexLabel("LDA");
        task.addInputField("s_text").addInputField("t_text");
        task.addOutputField("lda_sim");
        connectTask(task);
        return task;
    }
}
