package core;

import featurePipeline.DDFPipeline;
import featurePipeline.TraceModelPipeline;
import featurePipeline.SDFPipeline;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import traceability.components.abstractComponents.TraceArtifact;
import traceability.components.abstractComponents.TraceLink;
import traceability.components.basic.BasicTraceLink;

import javax.annotation.Nullable;
import java.io.Serializable;
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

    public SparkTraceJob(SparkSession session) {
        this.sparkSession = session;
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

        Dataset<Row> sourceSDFeatureVecs = sourceSDFPipelien.apply(sourceArtifacts);
        Dataset<Row> targetSDFeatureVecs = targetSDFPipeline.apply(targetArtifacts);
        Dataset<Row> goldLinksWithFeatureVec = appendFeaturesToLinks(goldenLinks.toDF(), sourceSDFeatureVecs, targetSDFeatureVecs);
        ddfPipeline.fit(goldLinksWithFeatureVec);

        Dataset<Row> fullFeatures = ddfPipeline.apply(goldLinksWithFeatureVec);
        //TODO Add feature selection/merging at this spot, collection features into a single vector for model processing.
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
        candidateLinks = appendFeaturesToLinks(candidateLinks, sourceFeatures, targetFeatures);
        Dataset<Row> finalFeatures = ddfPipeline.apply(candidateLinks);

        Dataset<Row> linkWithScores = modelPipeline.apply(finalFeatures);


        linkWithScores = linkWithScores.select("commit_id", "issue_id", "score");
        linkWithScores = linkWithScores.withColumnRenamed("commit_id", "sourceArtifactID");
        linkWithScores = linkWithScores.withColumnRenamed("issue_id", "targetArtifactID");
        linkWithScores = linkWithScores.withColumnRenamed("score", "label");
        linkWithScores.sort(linkWithScores.col("label").desc());
        List<Row> rows = linkWithScores.collectAsList();
        return null;
    }

    private Dataset<Row> appendFeaturesToLinks(Dataset<Row> links, Dataset<Row> sourceFeatures, Dataset<Row> targetFeatures) {
        Column sourceArtifactIdCol = sourceFeatures.col(sourceSDFPipelien.getIdColName());
        Column targetArtifactIdCol = targetFeatures.col(targetSDFPipeline.getIdColName());

        Column linkSourceIdCol = links.col(ddfPipeline.getSourceArtifactColName());
        Column linkTargetIdCol = links.col(ddfPipeline.getTargetArtifactColName());

        Dataset<Row> linksWithFeatureVec = links.join(sourceFeatures, sourceArtifactIdCol.equalTo(linkSourceIdCol));
        linksWithFeatureVec = linksWithFeatureVec.join(targetFeatures, targetArtifactIdCol.equalTo(linkTargetIdCol));
        linksWithFeatureVec = linksWithFeatureVec.drop(sourceArtifactIdCol).drop(targetArtifactIdCol);
        return linksWithFeatureVec;
    }

    public SparkSession getSparkSession() {
        return sparkSession;
    }

    public void setSparkSession(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public SDFPipeline getSourceSDFPipelien() {
        return sourceSDFPipelien;
    }

    public void setSourceSDFPipelien(SDFPipeline sourceSDFPipelien) {
        this.sourceSDFPipelien = sourceSDFPipelien;
    }

    public SDFPipeline getTargetSDFPipeline() {
        return targetSDFPipeline;
    }

    public void setTargetSDFPipeline(SDFPipeline targetSDFPipeline) {
        this.targetSDFPipeline = targetSDFPipeline;
    }

    public DDFPipeline getDdfPipeline() {
        return ddfPipeline;
    }

    public void setDdfPipeline(DDFPipeline ddfPipeline) {
        this.ddfPipeline = ddfPipeline;
    }

    public TraceModelPipeline getModelPipeline() {
        return modelPipeline;
    }

    public void setModelPipeline(TraceModelPipeline modelPipeline) {
        this.modelPipeline = modelPipeline;
    }
}
