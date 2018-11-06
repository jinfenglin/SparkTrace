package core;

import featurePipeline.DDFPipeline;
import featurePipeline.TraceModelPipeline;
import featurePipeline.SDFPipeline;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import traceability.components.abstractComponents.TraceArtifact;
import traceability.components.abstractComponents.TraceLink;
import traceability.components.basic.BasicTraceLink;

import java.util.List;

public class SparkTraceJob {
    protected SparkSession sparkSession;
    private SDFPipeline sourceSDFPipelien, targetSDFPipeline;
    private DDFPipeline ddfPipeline;
    private TraceModelPipeline modelPipeline;

    public SparkTraceJob(String masterUrl, String jobName) {
        SparkConf conf = new SparkConf();
        conf.setMaster(masterUrl);
        conf.setAppName(jobName);
        sparkSession = SparkSession.builder().config(conf).getOrCreate();
    }

    /**
     * Train trace model with Spark. If the model is unsupervised, which means only a batch of documents
     * should be applied for training, then the documents should be passed through sourceArtifacts parameter,
     * and the target Artifacts and  goldLinks can be null.
     *
     * @param sourceArtifacts
     * @param targetArtifacts
     * @param goldenLinks
     */
    public void trainSupervisedModel(Dataset<? extends TraceArtifact> sourceArtifacts,
                                     Dataset<? extends TraceArtifact> targetArtifacts,
                                     Dataset<? extends TraceLink> goldenLinks) {
        assert sourceArtifacts != null;
        sourceSDFPipelien.fit(sourceArtifacts);
        targetSDFPipeline.fit(targetArtifacts);

        //TODO Prototyping.... It is not runnable yet
        Dataset<Row> sourceSDFeatureVecs = sourceSDFPipelien.apply(sourceArtifacts);
        Dataset<Row> targetSDFeatureVecs = targetSDFPipeline.apply(targetArtifacts);

        Dataset<Row> goldLinksWithFeatureVec = goldenLinks.join(sourceSDFeatureVecs);
        goldLinksWithFeatureVec = goldLinksWithFeatureVec.join(targetSDFeatureVecs);
        ddfPipeline.fit(goldLinksWithFeatureVec);

        Dataset<Row> fullFeatures = ddfPipeline.apply(goldLinksWithFeatureVec);
        modelPipeline.fit(fullFeatures);
    }

    public void trainUnsupervisedModel(Dataset<? extends TraceArtifact> trainingArtifacts) {
        trainSupervisedModel(trainingArtifacts, null, null);
    }

    public List<BasicTraceLink> trace(Dataset<? extends TraceArtifact> sourceArtifacts,
                                      Dataset<? extends TraceArtifact> targetArtifacts) {
        Dataset<Row> sourceFeatures = sourceSDFPipelien.apply(sourceArtifacts);
        Dataset<Row> targetFeatures = targetSDFPipeline.apply(targetArtifacts);
        Dataset<Row> candidateLinks = sourceArtifacts.crossJoin(targetArtifacts); //Cross join
        candidateLinks = candidateLinks.join(sourceFeatures);
        candidateLinks = candidateLinks.join(targetFeatures);
        Dataset<Row> finalFeatures = ddfPipeline.apply(candidateLinks);
        Dataset<Row> linkWithScores = modelPipeline.apply(finalFeatures);
        Encoder<BasicTraceLink> basicTraceLinkEncoder = Encoders.bean(BasicTraceLink.class);
        return linkWithScores.as(basicTraceLinkEncoder).collectAsList();
    }


}
