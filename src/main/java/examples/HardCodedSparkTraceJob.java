package examples;

import core.SparkTraceJob;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

/**
 * A hard coded example of running a vsm job
 */
public class HardCodedSparkTraceJob extends SparkTraceJob {
    public HardCodedSparkTraceJob() {
        super("local[*]", "hardCodedExample");
    }

    public void trace() {
        List<Row> data = Arrays.asList(
                RowFactory.create(0.0, "Welcome to TutorialKart."),
                RowFactory.create(0.0, "Learn Spark at TutorialKart."),
                RowFactory.create(1.0, "Spark Mllib has TF-IDF.")
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("sentence", DataTypes.StringType, false, Metadata.empty())
        });

        // import data with the schema
        Dataset<Row> sentenceData = sparkSession.createDataFrame(data, schema);

        Tokenizer tokenizer = new Tokenizer().setInputCol("sentence");
        HashingTF tf = new HashingTF().setInputCol(tokenizer.getOutputCol());
        IDF idf = new IDF().setInputCol(tf.getOutputCol());
        Pipeline sdf = new Pipeline().setStages(new PipelineStage[]{tokenizer, tf, idf});
        PipelineModel sdfModel = sdf.fit(sentenceData);
        Dataset<Row> rescaledData = sdfModel.transform(sentenceData);

        System.out.println("Transformations\n----------------------------------------");
        for(Row row:rescaledData.collectAsList()){
            System.out.println(row);
        }
    }

    public static void main(String[] args) {
        HardCodedSparkTraceJob job = new HardCodedSparkTraceJob();
        job.trace();
    }

}
