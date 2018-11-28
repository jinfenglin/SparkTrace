package core;

import featurePipeline.*;
import org.apache.spark.SparkConf;
import org.apache.spark.ml.*;
import org.apache.spark.ml.clustering.LDA;
import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.param.ParamValidators;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import traceability.components.abstractComponents.TraceArtifact;
import traceability.components.abstractComponents.TraceLink;
import traceability.components.basic.BasicTraceLink;

import java.util.List;

public class SparkTraceJob {
    private static final long serialVersionUID = -5857405130938637355L;
    protected SparkSession sparkSession;
    private Pipeline sourceSDFPipeline, targetSDFPipeline;
    private Pipeline ddfPipeline;
    private TraceModelPipeline modelPipeline;

    public SparkTraceJob(String masterUrl, String jobName) {
        SparkConf conf = new SparkConf();
        conf.setMaster(masterUrl);
        conf.setAppName(jobName);
        sparkSession = SparkSession.builder().config(conf).getOrCreate();
    }

    public SparkTraceJob(SparkSession session) {
        this.sparkSession = session;
    }

    /**
     * Train trace pipelineModel with Spark. If the pipelineModel is unsupervised, which means only a batch of documents
     * should be applied for training, then the documents should be passed through sourceArtifacts parameter,
     * and the target Artifacts and  goldLinks can be null.
     *
     * @param sourceArtifacts
     * @param targetArtifacts
     * @param goldenLinks
     */
//    public void trainSupervisedModel(Dataset<? extends TraceArtifact> sourceArtifacts,
//                                     Dataset<? extends TraceArtifact> targetArtifacts,
//                                     Dataset<? extends TraceLink> goldenLinks) {
//        assert sourceArtifacts != null;
//        sourceSDFPipeline.fit(sourceArtifacts);
//        targetSDFPipeline.fit(targetArtifacts);
//
//        Dataset<Row> sourceSDFeatureVecs = sourceSDFPipeline.apply(sourceArtifacts);
//        Dataset<Row> targetSDFeatureVecs = targetSDFPipeline.apply(targetArtifacts);
//        Dataset<Row> goldLinksWithFeatureVec = appendFeaturesToLinks(goldenLinks.toDF(), sourceSDFeatureVecs, targetSDFeatureVecs);
//        ddfPipeline.fit(goldLinksWithFeatureVec);
//
//        Dataset<Row> fullFeatures = ddfPipeline.apply(goldLinksWithFeatureVec);
//        //TODO Add feature selection/merging at this spot, collection features into a single vector for pipelineModel processing.
//        modelPipeline.fit(fullFeatures);
//    }
//
//
//    public void trainUnsupervisedModel(Dataset<? extends TraceArtifact> trainingArtifacts) {
//        trainSupervisedModel(trainingArtifacts, null, null);
//    }
//
//    public List<BasicTraceLink> trace(Dataset<? extends TraceArtifact> sourceArtifacts,
//                                      Dataset<? extends TraceArtifact> targetArtifacts) {
//        Dataset<Row> sourceFeatures = sourceSDFPipeline.apply(sourceArtifacts);
//        Dataset<Row> targetFeatures = targetSDFPipeline.apply(targetArtifacts);
//
//        Dataset<Row> candidateLinks = sourceArtifacts.crossJoin(targetArtifacts); //Cross join
//        candidateLinks = appendFeaturesToLinks(candidateLinks, sourceFeatures, targetFeatures);
//        Dataset<Row> finalFeatures = ddfPipeline.apply(candidateLinks);
//
//        Dataset<Row> linkWithScores = modelPipeline.apply(finalFeatures);
//
//
//        linkWithScores = linkWithScores.select("commit_id", "issue_id", "score");
//        linkWithScores = linkWithScores.withColumnRenamed("commit_id", "sourceArtifactID");
//        linkWithScores = linkWithScores.withColumnRenamed("issue_id", "targetArtifactID");
//        linkWithScores = linkWithScores.withColumnRenamed("score", "label");
//        linkWithScores.sort(linkWithScores.col("label").desc());
//        List<Row> rows = linkWithScores.collectAsList();
//        return null;
//    }


    public SparkSession getSparkSession() {
        return sparkSession;
    }

    public void setSparkSession(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

}
