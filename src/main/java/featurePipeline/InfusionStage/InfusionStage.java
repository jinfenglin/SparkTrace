package featurePipeline.InfusionStage;

import core.graphPipeline.SDF.SDFGraph;
import core.graphPipeline.basic.SGraph;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.BooleanParam;
import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.param.StringArrayParam;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.apache.spark.sql.functions.col;

/**
 * A pipeline stage which transform the output of SDF pipeline to DDF pipeline input format
 */
public class InfusionStage extends Transformer implements InfusionStageParam {
    private static final long serialVersionUID = -9174712850938800008L;
    private BooleanParam trainingFlag;
    private StringArrayParam inputCols, outputCols;
    private Param<String> sourceIdCol, targetIdCol;
    private Dataset<Row> goldenLinks;
    private SDFGraph sdfGraph;
    private SGraph ddfGraph;

    public InfusionStage(SDFGraph sdfGraph, SGraph ddfGraph) {
        inputCols = initInputCols();
        outputCols = initOutputCols();
        trainingFlag = initTrainingFlag();
        sourceIdCol = initSourceIdCol();
        targetIdCol = initTargetIdCol();
        this.sdfGraph = sdfGraph;
        this.ddfGraph = ddfGraph;
    }

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        Dataset<Row> mixedSDFeatureVecs = (Dataset<Row>) dataset;
        Dataset<Row> sourceSDFeatureVecs = getSourceSDFFeatureVecs(mixedSDFeatureVecs);
        Dataset<Row> targetSDFeatureVecs = getTargetSDFFeatureVecs(mixedSDFeatureVecs);
        if (getTrainingFlag()) {
            Dataset<Row> goldLinksWithFeatureVec = null;
            if (goldenLinks == null) {
                List<StructField> fields = new ArrayList<>();
                for (StructField sourceField : sourceSDFeatureVecs.schema().fields()) {
                    fields.add(sourceField);
                }
                for (StructField targetField : targetSDFeatureVecs.schema().fields()) {
                    fields.add(targetField);
                }
                StructType emptyDFSchema = new StructType(fields.toArray(new StructField[0]));
                goldLinksWithFeatureVec = SparkSession.builder().getOrCreate().createDataFrame(new ArrayList<>(), emptyDFSchema);
            } else {
                goldLinksWithFeatureVec = appendFeaturesToLinks(goldenLinks.toDF(), sourceSDFeatureVecs, targetSDFeatureVecs);
            }
            return goldLinksWithFeatureVec;
        } else {
            Dataset<Row> candidateLinks = sourceSDFeatureVecs.crossJoin(targetSDFeatureVecs); //Cross join
            return candidateLinks;
        }
    }

    private Dataset<Row> getSourceSDFFeatureVecs(Dataset<Row> mixedSDFeatureVecs) {
        Set<String> sourceSDFCols = sdfGraph.getSourceSDFOutput();
        String sourceIdColName = getSourceIdCol();
        sourceSDFCols.add(sourceIdColName);
        Seq<String> sourceFeatureCols = JavaConverters.asScalaIteratorConverter(sourceSDFCols.iterator()).asScala().toSeq();
        return mixedSDFeatureVecs.selectExpr(sourceFeatureCols).where(col(sourceIdColName).isNotNull());
    }

    private Dataset<Row> getTargetSDFFeatureVecs(Dataset<Row> mixedSDFeatureVecs) {
        Set<String> targetSDFCols = sdfGraph.getTargetSDFOutputs();
        String targetIdColName = getTargetIdCol();
        targetSDFCols.add(targetIdColName);
        Seq<String> targetFeatureCols = JavaConverters.asScalaIteratorConverter(targetSDFCols.iterator()).asScala().toSeq();
        return mixedSDFeatureVecs.selectExpr(targetFeatureCols).where(col(targetIdColName).isNotNull());
    }

    private Dataset<Row> appendFeaturesToLinks(Dataset<Row> links, Dataset<Row> sourceFeatures, Dataset<Row> targetFeatures) {
        String sourceIDColName = getSourceIdCol();
        String targetIDColName = getTargetIdCol();
        Column sourceArtifactIdCol = sourceFeatures.col(sourceIDColName);
        Column targetArtifactIdCol = targetFeatures.col(targetIDColName);

        Column linkSourceIdCol = links.col(sourceIDColName);
        Column linkTargetIdCol = links.col(targetIDColName);

        Dataset<Row> linksWithFeatureVec = links.join(sourceFeatures, sourceArtifactIdCol.equalTo(linkSourceIdCol));
        linksWithFeatureVec = linksWithFeatureVec.join(targetFeatures, targetArtifactIdCol.equalTo(linkTargetIdCol));
        linksWithFeatureVec = linksWithFeatureVec.drop(sourceArtifactIdCol).drop(targetArtifactIdCol);
        return linksWithFeatureVec;
    }

    @Override
    public StructType transformSchema(StructType structType) {
        Set<String> sourceSDFCols = sdfGraph.getSourceSDFOutput();
        Set<String> targetSDFCols = sdfGraph.getTargetSDFOutputs();
        //TODO mismatch between real schema and return schema
        return structType;
    }

    @Override
    public Transformer copy(ParamMap paramMap) {
        return defaultCopy(paramMap);
    }

    @Override
    public String uid() {
        return String.valueOf(serialVersionUID);
    }

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public Dataset<Row> getGoldenLinks() {
        return goldenLinks;
    }

    public void setGoldenLinks(Dataset<Row> goldenLinks) {
        this.goldenLinks = goldenLinks;
    }


    @Override
    public void org$apache$spark$ml$param$shared$HasInputCols$_setter_$inputCols_$eq(StringArrayParam stringArrayParam) {

    }

    @Override
    public StringArrayParam inputCols() {
        return this.inputCols;
    }

    @Override
    public void org$apache$spark$ml$param$shared$HasOutputCols$_setter_$outputCols_$eq(StringArrayParam stringArrayParam) {

    }

    @Override
    public StringArrayParam outputCols() {
        return this.outputCols;
    }

    @Override
    public BooleanParam trainingFlag() {
        return this.trainingFlag;
    }

    @Override
    public Param<String> sourceIdCol() {
        return sourceIdCol;
    }

    @Override
    public Param<String> targetIdCol() {
        return targetIdCol;
    }
}
