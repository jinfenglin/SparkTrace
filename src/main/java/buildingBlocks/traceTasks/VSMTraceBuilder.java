package buildingBlocks.traceTasks;

import buildingBlocks.preprocessor.CleanTokens;
import buildingBlocks.preprocessor.SimpleWordCount;
import buildingBlocks.unsupervisedLearn.IDFGraphPipeline;
import buildingBlocks.vecSimilarityPipeline.SparseCosinSimilarityPipeline;
import core.SparkTraceTask;
import core.graphPipeline.basic.SGraph;

/**
 *
 */
public class VSMTraceBuilder implements TraceTaskBuilder {
    public static String INPUT_TEXT1 = "s_text";
    public static String INPUT_TEXT2 = "t_text";
    public static String OUTPUT = "vsm_sim";

    @Override
    public SGraph createSDF() throws Exception {
        return null;
    }

    public SGraph createSSDF() throws Exception {
        return SimpleWordCount.getGraph("VSM_SSDF");//text1,2 = "htf"
    }

    public SGraph createTSDF() throws Exception {
        return SimpleWordCount.getGraph("VSM_TSDF");//text = "htf"
    }

    @Override
    public SGraph createDDF() throws Exception {
        return SparseCosinSimilarityPipeline.getGraph("VSM_DDF"); //vec1,2 - cosin_sim
    }

    @Override
    public SparkTraceTask connectTask(SparkTraceTask task) throws Exception {
        task.connect(task.sourceNode, INPUT_TEXT1, task.getSourceSDFSdfGraph(), SimpleWordCount.INPUT_TEXT_COL);
        task.connect(task.sourceNode, INPUT_TEXT2, task.getTargetSDFSdfGraph(), SimpleWordCount.INPUT_TEXT_COL);
        task.connect(task.getSourceSDFSdfGraph(), SimpleWordCount.OUTPUT_HTF, task.getUnsupervisedLearnGraph(), IDFGraphPipeline.INPUT1);
        task.connect(task.getTargetSDFSdfGraph(), SimpleWordCount.OUTPUT_HTF, task.getUnsupervisedLearnGraph(), IDFGraphPipeline.INPUT2);
        task.connect(task.getUnsupervisedLearnGraph(), IDFGraphPipeline.OUTPUT1, task.getDdfGraph(), SparseCosinSimilarityPipeline.INPUT1);
        task.connect(task.getUnsupervisedLearnGraph(), IDFGraphPipeline.OUTPUT2, task.getDdfGraph(), SparseCosinSimilarityPipeline.INPUT2);
        task.connect(task.getDdfGraph(), SparseCosinSimilarityPipeline.OUTPUT, task.sinkNode, OUTPUT);
        return task;
    }

    @Override
    public SparkTraceTask getTask(String sourceId, String targetId) throws Exception {
        SGraph unsupervised = IDFGraphPipeline.getGraph("SharedIDF");
        SparkTraceTask task = new SparkTraceTask(createSSDF(), createTSDF(), unsupervised, createDDF(), sourceId, targetId);
        task.setVertexLabel("VSM");
        task.addInputField(INPUT_TEXT1).addInputField(INPUT_TEXT2);
        task.addOutputField(OUTPUT);
        connectTask(task);
        return task;
    }
}
