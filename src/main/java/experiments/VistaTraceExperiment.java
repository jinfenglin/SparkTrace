package experiments;

import core.SparkTraceJob;
import core.SparkTraceTask;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import traceTasks.LDATraceBuilder;
import traceTasks.NGramVSMTraceTaskBuilder;
import traceTasks.TraceTaskBuilder;
import traceTasks.VSMTraceBuilder;
import utils.DataReadUtil;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static core.graphPipeline.basic.Graph.syncSymbolValues;
import static utils.DataReadUtil.*;

/**
 *
 */
public class VistaTraceExperiment extends SparkTraceJob {
    int batch_size = 8000;
    List<Path> codeFilePaths;
    Dataset<Row> requirement, CCHIT, HIPPA;
    private static String outputDir = "results/vista";
    String sourceId = "s_id", targetId = "t_id";
    TraceTaskBuilder tracerBuilder;

    public VistaTraceExperiment(String codePath, String reqPath, String cchitPath, String hippaPath) throws Exception {
        super("local[*]", "Vista");
        //code = DataReadUtil.readVistaCode(codePath, sparkSession);
        codeFilePaths = Files.walk(Paths.get(codePath)).filter(Files::isRegularFile).collect(Collectors.toList());
        requirement = DataReadUtil.readVistaReq(reqPath, sparkSession).cache();
        CCHIT = DataReadUtil.readVistaCCHIT(cchitPath, sparkSession).cache();
        HIPPA = DataReadUtil.readVistaHIPPA(hippaPath, sparkSession).cache();
        requirement.cache().count();
        CCHIT.cache().count();
        HIPPA.cache().count();
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
        long start = System.currentTimeMillis();
        int index = 0;
        for (int i = 0; i <= iterNum; i++) {
            long batch_start = System.currentTimeMillis();
//            List<Row> rows = readCode(codeFilePaths.subList(i * batch_size, Math.min((i + 1) * batch_size, codeFilePaths.size())));
            List<Row> rows = readCode(codeFilePaths.subList(index, Math.min(index + batch_size, codeFilePaths.size())));
            index += batch_size;
            Dataset code = createVistaDataset(CODE_ID, CODE_CONTENT, rows, sparkSession);
            trace(CODE_ID, CODE_CONTENT, REQ_ID, REQ_CONTENT, code, requirement, outputDir + "code_req_" + i);
            long batch_end = System.currentTimeMillis();
            Logger.getLogger("").info(String.format("code-req batch %s time = %s", i, batch_end - batch_start));
            code.unpersist();
        }
        long end = System.currentTimeMillis();
        return end - start;
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

    public static void main(String[] args) throws Exception {
        String codePath = "G:\\Download\\VistA-M-master";
        String reqPath = "G:\\Download\\Vista\\Processed\\vista_requirement.csv"; // # = 1115
        String cchitPath = "G:\\Download\\Vista\\Processed\\Processed-CCHIT-NEW-For-Poirot.xml"; //# = 462
        String hippaPath = "G:\\Download\\Vista\\Processed\\11HIPAA_Goal_Model.xml"; // # = 10
        VistaTraceExperiment exp = new VistaTraceExperiment(codePath, reqPath, cchitPath, hippaPath);
        long t1 = 0, t2 = 0, t3 = 0, t4 = 0, t5 = 0, t6 = 0;

//        t1 = exp.TraceCodeHIPPA();
//        t2 = exp.traceCodeReq(); //SC->REQ
//        t3 = exp.TraceCodeCCHIT();
        t5 = exp.TraceReqCCHIT();//REQ->CHT
//        t4 = exp.TraceReqHIPPA(); //HP->REQ
//        t6 = exp.TraceHIPPAToCCHIT(); //HP->CHT
        String info = String.format("code_hippa:%s, code_req:%s, code_cchit:%s,req_hippa:%s,req_cchit:%s, hippa_cchit:%s", t1, t2, t3, t4, t5, t6);
        Files.write(Paths.get(outputDir + "/time.txt"), info.getBytes());

    }
}
