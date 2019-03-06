package experiments;


import core.SparkTraceJob;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.Seq;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;

public class DirtyBitExperiment extends SparkTraceJob {
    Dataset<Row> sourceDataset, targetDataset;

    public DirtyBitExperiment(String sourcePath, String targetPath, double sourceDirtPercent, double targetDirtPercent) {
        super("local[4]", "DirtyBit Experiment");//commit - issue
        sourceDataset = sparkSession.read().option("header", "true").csv(sourcePath); //commit
        targetDataset = sparkSession.read().option("header", "true").csv(targetPath); //issue
        prepareData(sourceDataset, sourceDirtPercent);
    }


    public void prepareData(Dataset dataset, double dirtyPercent) {
        dataset = dataset.orderBy(rand());
        Dataset selectedRows = dataset.select("commit_id").limit((int) (dataset.count() * dirtyPercent)).withColumn("dirtFlag", lit(true));
        dataset = dataset.orderBy(rand());
        Seq<String> colNames = scala.collection.JavaConverters.asScalaIteratorConverter(
                Arrays.asList("commit_id").iterator()
        ).asScala().toSeq();
        dataset = dataset.join(selectedRows, colNames, "left_outer");
        dataset = dataset.na().fill(false, new String[]{"dirtFlag"});
        dataset.show();
    }

    public static void main(String[] args) {
        String sourcePath = "src/main/resources/maven_mini/commits.csv";
        String targetPath = "src/main/resources/maven_mini/improvement.csv";
        DirtyBitExperiment exp = new DirtyBitExperiment(sourcePath, targetPath, 0.5, 0.5);

    }
}
