package traceTasks;

import componentRepo.SLayer.buildingBlocks.preprocessor.SimpleWordCount;
import componentRepo.SLayer.buildingBlocks.unsupervisedLearn.IDFGraphPipeline;
import componentRepo.SLayer.buildingBlocks.vecSimilarityPipeline.SparseCosinSimilarityPipeline;
import core.SparkTraceTask;
import core.graphPipeline.SLayer.SGraph;

import java.util.Arrays;

/**
 *
 */
public class VSMTraceBuilder implements TraceTaskBuilder {
    private boolean debug = false;
    public static String INPUT_TEXT1 = "s_text";
    public static String INPUT_TEXT2 = "t_text";
    public static String OUTPUT = "vsm_sim";

    public static String DEBUG_OUTPUT1 = "s_idf";
    public static String DEBUG_OUTPUT2 = "t_idf";

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
        task.connect(task.getSourceSDFSdfGraph(), SimpleWordCount.OUTPUT_HTF, task.getUnsupervisedLearnGraph().get(0), IDFGraphPipeline.INPUT1);
        task.connect(task.getTargetSDFSdfGraph(), SimpleWordCount.OUTPUT_HTF, task.getUnsupervisedLearnGraph().get(0), IDFGraphPipeline.INPUT2);
        task.connect(task.getUnsupervisedLearnGraph().get(0), IDFGraphPipeline.OUTPUT1, task.getDdfGraph(), SparseCosinSimilarityPipeline.INPUT1);
        task.connect(task.getUnsupervisedLearnGraph().get(0), IDFGraphPipeline.OUTPUT2, task.getDdfGraph(), SparseCosinSimilarityPipeline.INPUT2);
        task.connect(task.getDdfGraph(), SparseCosinSimilarityPipeline.OUTPUT, task.sinkNode, OUTPUT);
        if (debug) {
            task.connect(task.getUnsupervisedLearnGraph().get(0), IDFGraphPipeline.OUTPUT1, task.sinkNode, DEBUG_OUTPUT1);
            task.connect(task.getUnsupervisedLearnGraph().get(0), IDFGraphPipeline.OUTPUT2, task.sinkNode, DEBUG_OUTPUT2);
        }
        return task;
    }

    public SGraph createUnsupervised() throws Exception {
        return IDFGraphPipeline.getGraph("SharedIDF");
    }

    @Override
    public SparkTraceTask getTask(String sourceId, String targetId) throws Exception {
        SparkTraceTask task = new SparkTraceTask(createSSDF(), createTSDF(), Arrays.asList(createUnsupervised()), createDDF(), sourceId, targetId);
        task.setVertexLabel("VSM");
        task.addInputField(INPUT_TEXT1).addInputField(INPUT_TEXT2);
        task.addOutputField(OUTPUT);
        if (debug) {
            task.addOutputField(DEBUG_OUTPUT1).addOutputField(DEBUG_OUTPUT2);
        }
        connectTask(task);
        return task;
    }

    @Override
    public String getOutputColName() {
        return OUTPUT;
    }

    public void setDebugFlag(boolean flag) {
        this.debug = flag;
    }
}
