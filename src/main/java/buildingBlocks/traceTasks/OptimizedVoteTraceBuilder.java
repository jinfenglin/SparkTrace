package buildingBlocks.traceTasks;

import buildingBlocks.preprocessor.NGramCount;
import buildingBlocks.preprocessor.SimpleWordCount;
import buildingBlocks.unsupervisedLearn.IDFGraphPipeline;
import buildingBlocks.unsupervisedLearn.LDAGraphPipeline;
import buildingBlocks.vecSimilarityPipeline.DenseCosinSimilarityPipeline;
import buildingBlocks.vecSimilarityPipeline.SparseCosinSimilarityPipeline;
import core.SparkTraceTask;
import core.graphPipeline.basic.SGraph;

import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class OptimizedVoteTraceBuilder {

    public static String VOTE_IN1 = "s_text", VOTE_IN2 = "t_text";
    public static String VSM_SCORE = "vsm_score", NGRAM_SCORE = "vsm_ngram_score", LDA_SCORE = "lda_score";


    private String SSDF_IN1 = VOTE_IN1, SSDF_OUT1 = "s_vsm_htf", SSDF_OUT2 = "s_ngram_htf", SSDF_OUT3 = "s_lda_htf";
    private String TSDF_IN1 = VOTE_IN2, TSDF_OUT1 = "t_vsm_htf", TSDF_OUT2 = "t_ngram_htf", TSDF_OUT3 = "t_lda_htf";

    public SGraph createSSDF(SGraph vsm_SSDF, SGraph ngram_SSDF, SGraph LDA_SSDF) throws Exception {
        SGraph ssdf = new SGraph("Vote_SSDF");
        ssdf.addInputField(SSDF_IN1);
        ssdf.addOutputField(SSDF_OUT1);
        ssdf.addOutputField(SSDF_OUT2);
        ssdf.addOutputField(SSDF_OUT3);


        ssdf.addNode(vsm_SSDF);
        ssdf.addNode(ngram_SSDF);
        ssdf.addNode(LDA_SSDF);

        ssdf.connect(ssdf.sourceNode, SSDF_IN1, vsm_SSDF, SimpleWordCount.INPUT_TEXT_COL);
        ssdf.connect(ssdf.sourceNode, SSDF_IN1, ngram_SSDF, NGramCount.INPUT_TEXT_COL);
        ssdf.connect(ssdf.sourceNode, SSDF_IN1, LDA_SSDF, SimpleWordCount.INPUT_TEXT_COL);

        ssdf.connect(vsm_SSDF, SimpleWordCount.OUTPUT_HTF, ssdf.sinkNode, SSDF_OUT1);
        ssdf.connect(ngram_SSDF, NGramCount.OUTPUT_HTF, ssdf.sinkNode, SSDF_OUT2);
        ssdf.connect(LDA_SSDF, SimpleWordCount.OUTPUT_HTF, ssdf.sinkNode, SSDF_OUT3);
        return ssdf;
    }

    public SGraph createTSDF(SGraph vsm_TSDF, SGraph ngram_TSDF, SGraph LDA_TSDF) throws Exception {
        SGraph tsdf = new SGraph("Vote_TSDF");
        tsdf.addInputField(TSDF_IN1);
        tsdf.addOutputField(TSDF_OUT1);
        tsdf.addOutputField(TSDF_OUT2);
        tsdf.addOutputField(TSDF_OUT3);

        tsdf.addNode(vsm_TSDF);
        tsdf.addNode(ngram_TSDF);
        tsdf.addNode(LDA_TSDF);

        tsdf.connect(tsdf.sourceNode, TSDF_IN1, vsm_TSDF, SimpleWordCount.INPUT_TEXT_COL);
        tsdf.connect(tsdf.sourceNode, TSDF_IN1, ngram_TSDF, NGramCount.INPUT_TEXT_COL);
        tsdf.connect(tsdf.sourceNode, TSDF_IN1, LDA_TSDF, SimpleWordCount.INPUT_TEXT_COL);

        tsdf.connect(vsm_TSDF, SimpleWordCount.OUTPUT_HTF, tsdf.sinkNode, TSDF_OUT1);
        tsdf.connect(ngram_TSDF, NGramCount.OUTPUT_HTF, tsdf.sinkNode, TSDF_OUT2);
        tsdf.connect(LDA_TSDF, SimpleWordCount.OUTPUT_HTF, tsdf.sinkNode, TSDF_OUT3);
        return tsdf;
    }

    String s_vsm_idf = "s_vsm_idf", s_ngram_idf = "s_ngram_idf", s_lda_idf = "s_lda_idf";
    String t_vsm_idf = "t_vsm_idf", t_ngram_idf = "t_ngram_idf", t_lda_idf = "t_lda_idf";

    public SGraph createDDF(SGraph vsm_DDF, SGraph ngram_DDF, SGraph LDA_DDF) throws Exception {
        SGraph ddf = new SGraph("Vote_DDF");
        ddf.addInputField(s_vsm_idf);
        ddf.addInputField(s_ngram_idf);
        ddf.addInputField(s_lda_idf);
        ddf.addInputField(t_vsm_idf);
        ddf.addInputField(t_ngram_idf);
        ddf.addInputField(t_lda_idf);

        ddf.addOutputField(NGRAM_SCORE);
        ddf.addOutputField(VSM_SCORE);
        ddf.addOutputField(LDA_SCORE);


        ddf.addNode(vsm_DDF);
        ddf.addNode(ngram_DDF);
        ddf.addNode(LDA_DDF);

        ddf.connect(ddf.sourceNode, s_vsm_idf, vsm_DDF, SparseCosinSimilarityPipeline.INPUT1);
        ddf.connect(ddf.sourceNode, s_ngram_idf, ngram_DDF, SparseCosinSimilarityPipeline.INPUT1);
        ddf.connect(ddf.sourceNode, s_lda_idf, LDA_DDF, DenseCosinSimilarityPipeline.INPUT1);
        ddf.connect(ddf.sourceNode, t_vsm_idf, vsm_DDF, SparseCosinSimilarityPipeline.INPUT2);
        ddf.connect(ddf.sourceNode, t_ngram_idf, ngram_DDF, SparseCosinSimilarityPipeline.INPUT2);
        ddf.connect(ddf.sourceNode, t_lda_idf, LDA_DDF, DenseCosinSimilarityPipeline.INPUT2);

        ddf.connect(vsm_DDF, SparseCosinSimilarityPipeline.OUTPUT, ddf.sinkNode, VSM_SCORE);
        ddf.connect(ngram_DDF, SparseCosinSimilarityPipeline.OUTPUT, ddf.sinkNode, NGRAM_SCORE);
        ddf.connect(LDA_DDF, DenseCosinSimilarityPipeline.OUTPUT, ddf.sinkNode, LDA_SCORE);
        return ddf;
    }

    public SparkTraceTask connectTask(SparkTraceTask task) throws Exception {
        task.connect(task.sourceNode, VOTE_IN1, task.getSourceSDFSdfGraph(), SSDF_IN1);
        task.connect(task.sourceNode, VOTE_IN2, task.getTargetSDFSdfGraph(), TSDF_IN1);
        task.connect(task.getSourceSDFSdfGraph(), SSDF_OUT1, task.getUnsupervisedLearnGraph().get(0), IDFGraphPipeline.INPUT1);
        task.connect(task.getSourceSDFSdfGraph(), SSDF_OUT2, task.getUnsupervisedLearnGraph().get(1), IDFGraphPipeline.INPUT1);
        task.connect(task.getSourceSDFSdfGraph(), SSDF_OUT3, task.getUnsupervisedLearnGraph().get(2), LDAGraphPipeline.INPUT1);

        task.connect(task.getTargetSDFSdfGraph(), TSDF_OUT1, task.getUnsupervisedLearnGraph().get(0), IDFGraphPipeline.INPUT2);
        task.connect(task.getTargetSDFSdfGraph(), TSDF_OUT2, task.getUnsupervisedLearnGraph().get(1), IDFGraphPipeline.INPUT2);
        task.connect(task.getTargetSDFSdfGraph(), TSDF_OUT3, task.getUnsupervisedLearnGraph().get(2), LDAGraphPipeline.INPUT2);

        task.connect(task.getUnsupervisedLearnGraph().get(0), IDFGraphPipeline.OUTPUT1, task.getDdfGraph(), s_vsm_idf);
        task.connect(task.getUnsupervisedLearnGraph().get(1), IDFGraphPipeline.OUTPUT1, task.getDdfGraph(), s_ngram_idf);
        task.connect(task.getUnsupervisedLearnGraph().get(2), LDAGraphPipeline.OUTPUT1, task.getDdfGraph(), s_lda_idf);

        task.connect(task.getUnsupervisedLearnGraph().get(0), IDFGraphPipeline.OUTPUT2, task.getDdfGraph(), t_vsm_idf);
        task.connect(task.getUnsupervisedLearnGraph().get(1), IDFGraphPipeline.OUTPUT2, task.getDdfGraph(), t_ngram_idf);
        task.connect(task.getUnsupervisedLearnGraph().get(2), LDAGraphPipeline.OUTPUT2, task.getDdfGraph(), t_lda_idf);

        task.connect(task.getDdfGraph(), VSM_SCORE, task.sinkNode, VSM_SCORE);
        task.connect(task.getDdfGraph(), LDA_SCORE, task.sinkNode, LDA_SCORE);
        task.connect(task.getDdfGraph(), NGRAM_SCORE, task.sinkNode, NGRAM_SCORE);
        return task;
    }

    public SparkTraceTask getTask(String sourceId, String targetId) throws Exception {
        VSMTraceBuilder vsmBuilder = new VSMTraceBuilder();
        LDATraceBuilder ldaBuilder = new LDATraceBuilder(); //s_text lda_sim
        NGramVSMTraceTask ngram_vsmBuilder = new NGramVSMTraceTask();//s_text ngram_vsm_sim
        SGraph ssdf = createSSDF(vsmBuilder.createSSDF(), ngram_vsmBuilder.createSSDF(), ldaBuilder.createSSDF());
        SGraph tsdf = createTSDF(vsmBuilder.createTSDF(), ngram_vsmBuilder.createTSDF(), ldaBuilder.createTSDF());
        SGraph ddf = createDDF(vsmBuilder.createDDF(), ngram_vsmBuilder.createDDF(), ldaBuilder.createDDF());
        List<SGraph> unsueprvised = Arrays.asList(new SGraph[]{vsmBuilder.createUnsupervised(), ngram_vsmBuilder.createUnsupervise(), ldaBuilder.createUnsupervise()});
        SparkTraceTask task = new SparkTraceTask(ssdf, tsdf, unsueprvised, ddf, sourceId, targetId);

        task.setVertexLabel("vote");
        task.addInputField(VOTE_IN1).addInputField(VOTE_IN2);
        task.addOutputField(VSM_SCORE);
        task.addOutputField(LDA_SCORE);
        task.addOutputField(NGRAM_SCORE);
        connectTask(task);
        return task;

    }
}
