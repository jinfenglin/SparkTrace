import examples.TestBase;
import componentRepo.SLayer.featurePipelineStages.VecSimilarity.DenseVecSimilarity.DenseVecSimilarity;
import componentRepo.SLayer.featurePipelineStages.VecSimilarity.SparseVecSimilarity.SparseVecCosinSimilarityStage;
import componentRepo.SLayer.featurePipelineStages.sameColumnStage.SameAuthorStage;
import componentRepo.SLayer.featurePipelineStages.temporalRelations.CompareThreshold;
import componentRepo.SLayer.featurePipelineStages.temporalRelations.TimeDiff;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.clustering.LDA;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class CustomizedStagesTest extends TestBase {
    private static String masterUrl = "local";

    public CustomizedStagesTest() {
        super(masterUrl);
    }


    private Pipeline createTFPipeline(String columnPostfix) {
        Pipeline htfPipeline = new Pipeline();
        List<PipelineStage> stages = new ArrayList<>();
        Tokenizer tk = new Tokenizer();
        tk.setInputCol("text" + columnPostfix);
        tk.setOutputCol("tk" + columnPostfix);
        stages.add(tk);

        HashingTF htf = new HashingTF();
        htf.setInputCol("tk" + columnPostfix);
        htf.setOutputCol("htf" + columnPostfix);
        stages.add(htf);
        htfPipeline.setStages(stages.toArray(new PipelineStage[0]));
        return htfPipeline;
    }


    @Test
    public void CosinSimilarityTest() {
        String[] s1 = new String[]{"new york times", "new new york post", "los angeles times"};
        String[] s2 = new String[]{"new new times"};
        Dataset<Row> d1 = getSentenceDataset(Arrays.asList(s1));
        Dataset<Row> d2 = getSentenceDataset(Arrays.asList(s2));

        Pipeline tfPipeline = createTFPipeline("");
        List<PipelineStage> stages = new ArrayList<>();
        stages.addAll(Arrays.asList(tfPipeline.getStages()));

        IDF idf = new IDF("IDF");
        idf.setInputCol("htf");
        idf.setOutputCol("idf");
        stages.add(idf);


        Pipeline pipeline = new Pipeline().setStages(stages.toArray(new PipelineStage[0]));
        PipelineModel model = pipeline.fit(d1);
        d1 = model.transform(d1).withColumnRenamed("idf", "vec1");
        d2 = model.transform(d2).withColumnRenamed("idf", "vec2");

        Dataset<Row> dPair = d1.crossJoin(d2);

        SparseVecCosinSimilarityStage cosin = new SparseVecCosinSimilarityStage();
        cosin.setInputCols("vec1", "vec2");
        cosin.setOutputCol("score");
        cosin.transform(dPair).show(false);
    }

    @Test
    public void DenseCosinSimilarityTest() {
        String[] s1 = new String[]{"new york times", "new new york post", "los angeles times"};
        String[] s2 = new String[]{"new new times"};
        Dataset<Row> d1 = getSentenceDataset(Arrays.asList(s1));
        Dataset<Row> d2 = getSentenceDataset(Arrays.asList(s2));

        Pipeline tfPipeline = createTFPipeline("");
        List<PipelineStage> stages = new ArrayList<>();
        stages.addAll(Arrays.asList(tfPipeline.getStages()));

        LDA lda = new LDA("IDF");
        lda.setFeaturesCol("htf").setTopicDistributionCol("lda");
        stages.add(lda);

        Pipeline pipeline = new Pipeline().setStages(stages.toArray(new PipelineStage[0]));
        PipelineModel model = pipeline.fit(d1);
        d1 = model.transform(d1).withColumnRenamed("lda", "vec1");
        d2 = model.transform(d2).withColumnRenamed("lda", "vec2");

        Dataset<Row> dPair = d1.crossJoin(d2);

        DenseVecSimilarity cosin = new DenseVecSimilarity();
        cosin.setInputCols("vec1", "vec2");
        cosin.setOutputCol("score");
        cosin.transform(dPair).show(false);
    }

    @Test
    public void sameUserIdStage() {
        String[] s1 = new String[]{"name1", "name2", ""};
        String[] s2 = new String[]{"name2"};
        Dataset<Row> d1 = getSentenceDataset(Arrays.asList(s1)).withColumnRenamed("text", "text1");
        Dataset<Row> d2 = getSentenceDataset(Arrays.asList(s2)).withColumnRenamed("text", "text2");
        Dataset dataset = d1.crossJoin(d2);
        SameAuthorStage stage = new SameAuthorStage();
        stage.set(stage.inputCols(), new String[]{"text1", "text2"});
        stage.set(stage.outputCol(), "sameUser");
        stage.transform(dataset).show();
    }

    @Test
    public void timeDiffTest() {
        String[] s1 = new String[]{"Sun Apr 30 00:00:00 EDT 2006", "Mon Apr 10 00:00:00 EDT 2006", "Mon Apr 10 00:00:00 EDT 2006"};
        String[] s2 = new String[]{"Mon Apr 03 00:00:00 EDT 2006", "Sat Apr 01 00:00:00 EST 2006"};
        Dataset<Row> d1 = getSentenceDataset(Arrays.asList(s1)).withColumnRenamed("text", "time1");
        Dataset<Row> d2 = getSentenceDataset(Arrays.asList(s2)).withColumnRenamed("text", "time2");
        Dataset dataset = d1.crossJoin(d2);
        TimeDiff timeDiff = new TimeDiff();
        timeDiff.set(timeDiff.inputCols(), new String[]{"time1", "time2"});
        timeDiff.set(timeDiff.outputCol(), "timeDiff");
        dataset = timeDiff.transform(dataset);

        CompareThreshold ct = new CompareThreshold();
        ct.set(ct.getParam("threshold"), 10.0);
        ct.set(ct.inputCol(), "timeDiff");
        ct.set(ct.outputCol(), "thresholdCompare");
        ct.transform(dataset).show();
    }

}
