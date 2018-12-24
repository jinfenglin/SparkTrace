package core;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;


public class SparkTraceJob {
    private static final long serialVersionUID = -5857405130938637355L;
    protected SparkSession sparkSession;

    protected SparkTraceTask task; //Root task

    public SparkTraceJob(String masterUrl, String jobName) {
        SparkConf conf = new SparkConf();
        conf.setMaster(masterUrl);
        conf.setAppName(jobName);
        sparkSession = SparkSession.builder().config(conf).getOrCreate();
    }

    public SparkTraceJob(SparkSession session) {
        this.sparkSession = session;
    }

    public SparkSession getSparkSession() {
        return sparkSession;
    }

    public void setSparkSession(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

}
