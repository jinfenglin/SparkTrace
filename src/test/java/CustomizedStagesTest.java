import examples.TestBase;
import featurePipelineStages.LDAWithIO.LDAWithIO;
import featurePipelineStages.VecSimilarity.DenseVecSimilarity.DenseVecSimilarity;
import featurePipelineStages.VecSimilarity.SparseVecSimilarity.SparseVecCosinSimilarityStage;
import featurePipelineStages.NullRemoveWrapper.NullRemoverEstimatorSingleIO;
import featurePipelineStages.NullRemoveWrapper.NullRemoverModelSingleIO;
import featurePipelineStages.UnsupervisedStage.UnsupervisedStage;
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

    @Test
    public void NullRemoverConfigRemoverTest() {
        Dataset<Row> datasetWithNull = createDatasetWithNull();

        Tokenizer tk = new Tokenizer("tokenizer");
        NullRemoverModelSingleIO model = new NullRemoverModelSingleIO(tk);
        model.setInputCol("sentence");
        model.setOutputCol("tokens");
        assert ((Tokenizer) model.getInnerStage()).getInputCol().equals("sentence");
        assert ((Tokenizer) model.getInnerStage()).getOutputCol().equals("tokens");
        datasetWithNull = model.transform(datasetWithNull);

        HashingTF htf = new HashingTF("htf");
        model = new NullRemoverModelSingleIO(htf);
        model.setInputCol("tokens");
        model.setOutputCol("htf");
        datasetWithNull = model.transform(datasetWithNull);

        IDF idf = new IDF("IDF");
        NullRemoverEstimatorSingleIO nullRemoverIDF = new NullRemoverEstimatorSingleIO(idf);
        nullRemoverIDF.setInputCol("htf");
        nullRemoverIDF.setOutputCol("idf");
        nullRemoverIDF.fit(datasetWithNull).transform(datasetWithNull).show(false);
    }

    @Test
    public void NullRemoverConfigInnerStageTest() {
        Dataset<Row> datasetWithNull = createDatasetWithNull();

        Tokenizer tk = new Tokenizer();
        tk.setInputCol("sentence");
        tk.setOutputCol("tokens");
        NullRemoverModelSingleIO model = new NullRemoverModelSingleIO(tk);
        assert ((Tokenizer) model.getInnerStage()).getInputCol().equals("sentence");
        assert ((Tokenizer) model.getInnerStage()).getOutputCol().equals("tokens");
        datasetWithNull = model.transform(datasetWithNull);

        HashingTF htf = new HashingTF("htf");
        htf.setInputCol("tokens");
        htf.setOutputCol("htf");
        model = new NullRemoverModelSingleIO(htf);
        datasetWithNull = model.transform(datasetWithNull);

        IDF idf = new IDF("IDF");
        idf.setInputCol("htf");
        idf.setOutputCol("idf");
        NullRemoverEstimatorSingleIO nullRemoverIDF = new NullRemoverEstimatorSingleIO(idf);
        nullRemoverIDF.fit(datasetWithNull).transform(datasetWithNull).show(false);
    }

    private Pipeline createTFPipeline(String columnPostfix) {
        Pipeline htfPipeline = new Pipeline();
        List<PipelineStage> stages = new ArrayList<>();
        Tokenizer tk = new Tokenizer();
        tk.setInputCol("text" + columnPostfix);
        tk.setOutputCol("tk" + columnPostfix);
        stages.add(new NullRemoverModelSingleIO(tk));

        HashingTF htf = new HashingTF();
        htf.setInputCol("tk" + columnPostfix);
        htf.setOutputCol("htf" + columnPostfix);
        stages.add(new NullRemoverModelSingleIO(htf));
        htfPipeline.setStages(stages.toArray(new PipelineStage[0]));
        return htfPipeline;
    }

    @Test
    public void UnsupervisedStageTest() {
        Dataset<Row> twoColumnData = getMultiSentenceRowData();
        Pipeline pipeline1 = createTFPipeline("1");
        Pipeline pipeline2 = createTFPipeline("2");
        twoColumnData = pipeline1.fit(twoColumnData).transform(twoColumnData);
        twoColumnData = pipeline2.fit(twoColumnData).transform(twoColumnData);

        IDF idf = new IDF();
        UnsupervisedStage unsupervisedStage = new UnsupervisedStage(idf);
        unsupervisedStage.setInputCols(new String[]{"htf1", "htf2"});
        unsupervisedStage.setOutputCols(new String[]{"idf1", "idf2"});
        unsupervisedStage.fit(twoColumnData).transform(twoColumnData).show(false);
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

}
