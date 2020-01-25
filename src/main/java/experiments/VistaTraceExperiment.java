package experiments;

import core.SparkTraceJob;
import core.SparkTraceTask;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.*;
import traceTasks.LDATraceBuilder;
import traceTasks.NGramVSMTraceTaskBuilder;
import traceTasks.TraceTaskBuilder;
import traceTasks.VSMTraceBuilder;
import utils.DataReadUtil;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static core.graphPipeline.basic.Graph.syncSymbolValues;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static utils.DataReadUtil.*;

/**
 *
 */
public class VistaTraceExperiment extends SparkTraceJob {
    int batch_size = 8000;
    List<Path> codeFilePaths;
    Dataset<Row> requirement, CCHIT, HIPPA, packages;
    private static String outputDir = "results/vista";
    String sourceId = "s_id", targetId = "t_id";
    TraceTaskBuilder tracerBuilder;

    public VistaTraceExperiment(String codePath, String reqPath, String cchitPath, String hippaPath) throws Exception {
        super("local[*]", "Vista");
        //code = DataReadUtil.readVistaCode(codePath, sparkSession);
        codeFilePaths = Files.walk(Paths.get(codePath)).filter(Files::isRegularFile).collect(Collectors.toList());
        packages = readPackage(codePath, sparkSession);
        requirement = DataReadUtil.readVistaReq(reqPath, sparkSession).cache();
        CCHIT = DataReadUtil.readVistaCCHIT(cchitPath, sparkSession);
        HIPPA = DataReadUtil.readVistaHIPPA(hippaPath, sparkSession).cache();
        tracerBuilder = new VSMTraceBuilder();
    }

    protected Dataset trace(SparkTraceTask tracer, String s_id, String s_text, String t_id, String t_text, Dataset sourceData, Dataset targetData) throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put("s_text", s_text);
        config.put("t_text", t_text);
        config.put("s_id", s_id);
        config.put("t_id", t_id);
        tracer.setConfig(config);
        syncSymbolValues(tracer);
        tracer.train(sourceData, targetData, null);
        Dataset<Row> result = tracer.trace(sourceData, targetData);
        return result;
    }

    protected void trace(String s_id, String s_text, String t_id, String t_text, Dataset sourceData, Dataset targetData, String linkDir) throws Exception {
        SparkTraceTask tracer = tracerBuilder.getTask(sourceId, targetId);
        Dataset<Row> result = trace(tracer, s_id, s_text, t_id, t_text, sourceData, targetData);
        String vsmScore = tracer.getOutputField(tracerBuilder.getOutputColName()).getFieldSymbol().getSymbolValue();
        result.filter(col(vsmScore).gt(0.05));
        result.count();
//        result.select(s_id, t_id, vsmScore).write()
//                .format("csv")
//                .option("header", "true").mode("overwrite")
//                .save(linkDir);
        result.unpersist();
    }

//    public long traceCodeToOneReq() throws Exception {
//        long start = System.currentTimeMillis();
//        for (int i = 0; i < 1; i++) {
//            Dataset oneReqDf = requirement.limit(1);
//            List<Row> rows = readCode(codeFilePaths);
//            Dataset code = createVistaDataset(CODE_ID, CODE_CONTENT, rows, sparkSession);
//            trace(CODE_ID, CODE_CONTENT, REQ_ID, REQ_CONTENT, code, oneReqDf, "code_req");
//        }
//        long end = System.currentTimeMillis();
//        return end - start;
//    }

    public long traceCodeReq() throws Exception {
        int iterNum = codeFilePaths.size() / batch_size;
        long time = 0;
        int index = 0;
        requirement = requirement.limit(48).cache();
        for (int i = 0; i <= iterNum; i++) {
            List<Row> rows = readCode(codeFilePaths.subList(index, Math.min(index + batch_size, codeFilePaths.size())));
            index += batch_size;
            Dataset code = createVistaDataset(CODE_ID, CODE_CONTENT, rows, sparkSession);
            long batch_start = System.currentTimeMillis();
            trace(CODE_ID, CODE_CONTENT, REQ_ID, REQ_CONTENT, code, requirement, outputDir + "/code_req_" + i);
            long batch_end = System.currentTimeMillis();
            time += batch_end - batch_start;
            Logger.getLogger("").info(String.format("code-req batch %s time = %s", i, batch_end - batch_start));
            code.unpersist();
        }
        return time;
    }

    public long TraceCodeHIPPA() throws Exception {
        int iterNum = codeFilePaths.size() / batch_size;
        long start = System.currentTimeMillis();
        for (int i = 0; i < iterNum; i++) {
            long batch_start = System.currentTimeMillis();
            Logger.getLogger(this.getClass().getName()).warning(String.format("Processing code-HIPPA in batch %s", i));
            List<Row> rows = readCode(codeFilePaths.subList(i * batch_size, Math.min((i + 1) * batch_size, codeFilePaths.size())));
            Dataset code = createVistaDataset(CODE_ID, CODE_CONTENT, rows, sparkSession);
            trace(CODE_ID, CODE_CONTENT, HIPPA_ID, HIPPA_CONTENT, code, HIPPA, outputDir + "code_HIPPA_" + i);
            long batch_end = System.currentTimeMillis();
            Logger.getLogger("").info(String.format("code-req batch %s time = %s", i, batch_end - batch_start));
        }
        long end = System.currentTimeMillis();
        return end - start;
    }

    public long TraceCodeCCHIT() throws Exception {
        int iterNum = codeFilePaths.size() / batch_size;
        long start = System.currentTimeMillis();
        for (int i = 0; i < iterNum; i++) {
            long batch_start = System.currentTimeMillis();
            List<Row> rows = readCode(codeFilePaths.subList(i * batch_size, Math.min((i + 1) * batch_size, codeFilePaths.size())));
            Dataset code = createVistaDataset(CODE_ID, CODE_CONTENT, rows, sparkSession);
            trace(CODE_ID, CODE_CONTENT, CCHIT_ID, CCHIT_CONTENT, code, CCHIT, outputDir + "code_CCHIT" + i);
            long batch_end = System.currentTimeMillis();
            Logger.getLogger("").info(String.format("code-req batch %s time = %s", i, batch_end - batch_start));
        }
        long end = System.currentTimeMillis();
        return end - start;
    }

    public long TraceReqHIPPA() throws Exception {
        long start = System.currentTimeMillis();
        trace(REQ_ID, REQ_CONTENT, HIPPA_ID, HIPPA_CONTENT, requirement, HIPPA, outputDir + "req_HIPPA");
        long end = System.currentTimeMillis();
        return end - start;
    }

    public long TraceReqCCHIT() throws Exception {
        long start = System.currentTimeMillis();
        trace(REQ_ID, REQ_CONTENT, CCHIT_ID, CCHIT_CONTENT, requirement, CCHIT, outputDir + "req_CCHIT");
        long end = System.currentTimeMillis();
        return end - start;
    }


    public long TraceHIPPAToCCHIT() throws Exception {
        long start = System.currentTimeMillis();
        trace(CCHIT_ID, CCHIT_CONTENT, HIPPA_ID, HIPPA_CONTENT, CCHIT, HIPPA, outputDir + "HIPPA_CCHIT");
        long end = System.currentTimeMillis();
        return end - start;
    }

    public long traceQueryReq() throws Exception {
        //trace from architectural query to requirements and apply threshold
        //aggregate on the score on package
        String QUERY_ID = "query_id", QUERY_CONTENT = "query_content";
        String query = "messaging messages HL7 protocol synchronous asynchro-nous  tcp  transmit  send  transmission  receive  interfacememory router exchange transport A";
        Row query_row1 = RowFactory.create("1", query);
        Row query_row2 = RowFactory.create("2", "system database client server distributed SQL S");
        Dataset query_data = createVistaDataset(QUERY_ID, QUERY_CONTENT, Arrays.asList(query_row1, query_row2), sparkSession).cache();
        long start = System.currentTimeMillis();
        trace(QUERY_ID, QUERY_CONTENT, REQ_ID, REQ_CONTENT, query_data, requirement, outputDir + "query_req");
        long end = System.currentTimeMillis();
        return end - start;
    }

    public long aggregate() throws IOException {
        StructType schema = new StructType(new StructField[]{
                new StructField("code_id", DataTypes.StringType, true, Metadata.empty()),
                new StructField("req_id", DataTypes.StringType, true, Metadata.empty()),
                new StructField("vsm_score", DataTypes.DoubleType, true, Metadata.empty())
        });
        Dataset links = sparkSession.read().format("csv").option("header", "False").schema(schema).load("results/vista_q1_final/*/*.csv");
        Map<String, String> package_map = DataReadUtil.readPackageMap("G:\\Download\\VistA-M-master\\Packages");
        sparkSession.sqlContext().udf().register("assign_package", (String code_id) -> {
            return package_map.getOrDefault(code_id, "");
        }, DataTypes.StringType);
        List<String> req_set = requirement.limit(48).select("req_id").collectAsList().stream().map(x -> (String) x.get(0)).collect(Collectors.toList());
        links = links.filter(col("vsm_score").gt(0)).filter(col("req_id").isin(req_set.stream().toArray(String[]::new)));
        links = links.withColumn("package", callUDF("assign_package", links.col("code_id")));
        links = links.cache();
        System.out.println(String.format("link_num:%s", links.count()));
        long start_time = System.currentTimeMillis();
        Dataset result = links.groupBy("package").sum("vsm_score");
        result.count();
        long end_time = System.currentTimeMillis();
        return end_time - start_time;
    }


    public static void main(String[] args) throws Exception {
        String codePath = "G:\\Download\\VistA-M-master\\Packages";
        String reqPath = "G:\\Download\\Vista\\Processed\\vista_requirement.csv"; // # = 1115
        String cchitPath = "G:\\Download\\Vista\\Processed\\Processed-CCHIT-NEW-For-Poirot.xml"; //# = 462
        String hippaPath = "G:\\Download\\Vista\\Processed\\11HIPAA_Goal_Model.xml"; // # = 10
        VistaTraceExperiment exp = new VistaTraceExperiment(codePath, reqPath, cchitPath, hippaPath);
        long t1 = 0, t2 = 0, t3 = 0, t4 = 0, t5 = 0, t6 = 0, t7 = 0;
//        t1 = exp.traceQueryReq();
//        t1 = exp.TraceCodeHIPPA();
//        t2 = exp.traceCodeReq(); //SC->REQ
//        t3 = exp.TraceCodeCCHIT();
//        t5 = exp.TraceReqCCHIT();//REQ->CHT
//        t4 = exp.TraceReqHIPPA(); //HP->REQ
//        t6 = exp.TraceHIPPAToCCHIT(); //HP->CHT
        t7 = exp.aggregate();
        String info = String.format("code_hippa:%s, code_req:%s, code_cchit:%s,req_hippa:%s,req_cchit:%s, hippa_cchit:%s, aggregate:%s", t1, t2, t3, t4, t5, t6, t7);
        Files.write(Paths.get(outputDir + "/time.txt"), info.getBytes());
    }
}
