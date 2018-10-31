package core;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class SparkTraceJob {
    SparkSession sparkSession;

    public SparkTraceJob(String masterUrl, String jobName) {
        SparkConf conf = new SparkConf();
        conf.setMaster(masterUrl);
        conf.setAppName(jobName);
        sparkSession = SparkSession.builder().config(conf).getOrCreate();
    }
}
