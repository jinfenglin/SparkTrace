package examples;

import core.SparkTraceJob;


/**
 *
 */
public class test extends SparkTraceJob {
    public test(String masterUrl, String jobName) {
        super(masterUrl, jobName);
    }
    public static void main(String[] args) {
        System.out.println("hello");
    }
}
