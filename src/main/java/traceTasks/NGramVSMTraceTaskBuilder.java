package traceTasks;

import componentRepo.SLayer.buildingBlocks.preprocessor.NGramCount;
import componentRepo.SLayer.buildingBlocks.unsupervisedLearn.IDFGraphPipeline;
import componentRepo.SLayer.buildingBlocks.vecSimilarityPipeline.SparseCosinSimilarityPipeline;
import core.SparkTraceTask;
import core.graphPipeline.SLayer.SGraph;

import java.util.Arrays;

/**
 *
 */
public class NGramVSMTraceTaskBuilder implements TraceTaskBuilder {
    public static String INPUT1 = "s_text", INPUT2 = "t_text";
    public static String OUTPUT = "ngram_vsm_sim";

    public SGraph createSSDF() throws Exception {
        SGraph graph = NGramCount.getGraph("NGramVSM_SSDF");//text1,2 = "ngram-tf-idf1,2"
        return graph;
    }

    public SGraph createTSDF() throws Exception {
        SGraph graph = NGramCount.getGraph("NGramVSM_TSDF");//text1,2 = "ngram-tf-idf1,2"
        return graph;
    }

    @Override
    public SGraph createSDF() throws Exception {
        return null;
    }

    @Override
    public SGraph createDDF() throws Exception {
        return SparseCosinSimilarityPipeline.getGraph("NGramVSM_DDF"); //vec1,2 - cosin_sim
    }

    @Override
    public SparkTraceTask connectTask(SparkTraceTask task) throws Exception {
        task.connect(task.sourceNode, INPUT1, task.getSourceSDFSdfGraph(), NGramCount.INPUT_TEXT_COL);
        task.connect(task.sourceNode, INPUT2, task.getTargetSDFSdfGraph(), NGramCount.INPUT_TEXT_COL);
        task.connect(task.getSourceSDFSdfGraph(), NGramCount.OUTPUT_HTF, task.getUnsupervisedLearnGraph().get(0), IDFGraphPipeline.INPUT1);
        task.connect(task.getTargetSDFSdfGraph(), NGramCount.OUTPUT_HTF, task.getUnsupervisedLearnGraph().get(0), IDFGraphPipeline.INPUT2);
        task.connect(task.getUnsupervisedLearnGraph().get(0), IDFGraphPipeline.OUTPUT1, task.getDdfGraph(), SparseCosinSimilarityPipeline.INPUT1);
        task.connect(task.getUnsupervisedLearnGraph().get(0), IDFGraphPipeline.OUTPUT2, task.getDdfGraph(), SparseCosinSimilarityPipeline.INPUT2);
        task.connect(task.getDdfGraph(), SparseCosinSimilarityPipeline.OUTPUT, task.sinkNode, OUTPUT);
        return task;
    }

    public SGraph createUnsupervise() throws Exception {
        return IDFGraphPipeline.getGraph("SharedIDF");
    }

    @Override
    public SparkTraceTask getTask(String sourceId, String targetId) throws Exception {
        SparkTraceTask task = new SparkTraceTask(createSSDF(), createTSDF(), Arrays.asList(createUnsupervise()), createDDF(), sourceId, targetId);
        task.setVertexLabel("NGramVSM");
        task.addInputField(INPUT1).addInputField(INPUT2);
        task.addOutputField(OUTPUT);
        connectTask(task);
        return task;
    }

    @Override
    public String getOutputColName() {
        return OUTPUT;
    }
}
