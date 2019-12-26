package experiments;

import core.SparkTraceJob;
import core.SparkTraceTask;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import traceTasks.OptimizedVoteTraceBuilder;
import traceTasks.VSMTraceBuilder;
import utils.DataReadUtil;

import javax.xml.crypto.Data;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static core.graphPipeline.basic.Graph.syncSymbolValues;
import static utils.DataReadUtil.*;

/**
 *
 */
public class VistaTraceExperiment extends SparkTraceJob {
    List<Path> codeFilePaths;
    Dataset<Row> requirement, CCHIT, HIPPA;
    private static String outputDir = "results/vista";
    String sourceId = "s_id", targetId = "t_id";

    public VistaTraceExperiment(String codePath, String reqPath, String cchitPath, String hippaPath) throws IOException {
        super("local[*]", "Vista");
        //code = DataReadUtil.readVistaCode(codePath, sparkSession);
        codeFilePaths = Files.walk(Paths.get(codePath)).filter(Files::isRegularFile).collect(Collectors.toList());
        requirement = DataReadUtil.readVistaReq(reqPath, sparkSession);
        CCHIT = DataReadUtil.readVistaCCHIT(cchitPath, sparkSession);
        HIPPA = DataReadUtil.readVistaHIPPA(hippaPath, sparkSession);
    }

    private void traceWithVSM(String s_id, String s_text, String t_id, String t_text, Dataset sourceData, Dataset targetData, String linkDir) throws Exception {
        SparkTraceTask vsmTracer = new VSMTraceBuilder().getTask(sourceId, targetId);
        Map<String, String> config = new HashMap<>();
        config.put("s_text", s_text);
        config.put("t_text", t_text);
        config.put("s_id", s_id);
        config.put("t_id", t_id);
        vsmTracer.setConfig(config);
        syncSymbolValues(vsmTracer);
        vsmTracer.train(sourceData, targetData, null);
        Dataset<Row> result = vsmTracer.trace(sourceData, targetData);
        String vsmScore = vsmTracer.getOutputField(VSMTraceBuilder.OUTPUT).getFieldSymbol().getSymbolValue();
        result.select(config.get(sourceId), config.get(targetId), vsmScore).write()
                .format("com.databricks.spark.csv")
                .option("header", "true").mode("overwrite")
                .save(outputDir + "/" + linkDir);

    }

    public void traceCodeReq() throws Exception {
        int iterNum = codeFilePaths.size() / 5000;
        for (int i = 0; i < iterNum; i++) {
            List<Row> rows = readCode(codeFilePaths.subList(iterNum * 5000, Math.min((iterNum + 1) * 5000, codeFilePaths.size())));
            Dataset code = createVistaDataset(CODE_ID, CODE_CONTENT, rows, sparkSession);
            traceWithVSM(CODE_ID, CODE_CONTENT, REQ_ID, REQ_CONTENT, code, requirement, "code_req_" + String.valueOf(i));
        }
    }

    public void TraceCodeHIPPA() throws Exception {
        int iterNum = codeFilePaths.size() / 5000;
        for (int i = 0; i < iterNum; i++) {
            List<Row> rows = readCode(codeFilePaths.subList(iterNum * 5000, Math.min((iterNum + 1) * 5000, codeFilePaths.size())));
            Dataset code = createVistaDataset(CODE_ID, CODE_CONTENT, rows, sparkSession);
            traceWithVSM(CODE_ID, CODE_CONTENT, HIPPA_ID, HIPPA_CONTENT, code, HIPPA, "code_HIPPA");
        }
    }

    public void TraceCodeCCHIT() throws Exception {
        int iterNum = codeFilePaths.size() / 5000;
        for (int i = 0; i < iterNum; i++) {
            List<Row> rows = readCode(codeFilePaths.subList(iterNum * 5000, Math.min((iterNum + 1) * 5000, codeFilePaths.size())));
            Dataset code = createVistaDataset(CODE_ID, CODE_CONTENT, rows, sparkSession);
            traceWithVSM(CODE_ID, CODE_CONTENT, CCHIT_ID, CCHIT_CONTENT, code, CCHIT, "code_CCHIT");
        }
    }

    public void TraceReqHIPPA() throws Exception {
        traceWithVSM(REQ_ID, REQ_CONTENT, HIPPA_ID, HIPPA_CONTENT, requirement, HIPPA, "req_HIPPA");
    }

    public void TraceReqCCHIT() throws Exception {
        traceWithVSM(REQ_ID, REQ_CONTENT, CCHIT_ID, CCHIT_CONTENT, requirement, CCHIT, "req_CCHIT");
    }

    public static void main(String[] args) throws Exception {
        String codePath = "G:\\Download\\VistA-M-master";
        String reqPath = "G:\\Download\\Vista\\Processed\\Processed-Vista-NEW.xml";
        String cchitPath = "G:\\Download\\Vista\\Processed\\Processed-CCHIT-NEW-For-Poirot.xml";
        String hippaPath = "G:\\Download\\Vista\\Processed\\11HIPAA_Goal_Model.xml";
        VistaTraceExperiment exp = new VistaTraceExperiment(codePath, reqPath, cchitPath, hippaPath);
        exp.TraceCodeHIPPA();
//        exp.traceCodeReq();
//        exp.TraceCodeHIPPA();
//        exp.TraceCodeCCHIT();
//        exp.TraceReqHIPPA();
//        exp.TraceReqCCHIT();
    }
}
