package experiments;

import componentRepo.SLayer.featurePipelineStages.cleanStage.CleanStage;
import core.SparkTraceTask;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import traceTasks.VSMTraceBuilder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static core.graphPipeline.basic.Graph.syncSymbolValues;
import static org.apache.spark.sql.functions.*;
import static utils.DataReadUtil.*;
import static utils.DataReadUtil.REQ_CONTENT;

/**
 *
 */
public class ICPC_Query2 extends VistaTraceExperiment {
    private static String outputDir = "results/vista_query2";
    private static String secure_code_dir = outputDir + "/" + "security_code";
    private static String code_req_link_dir = outputDir + "/" + "code_req_links";
    Dataset<Row> query_data;
    String QUERY_ID = "query_id", QUERY_CONTENT = "query_content";
    String codePath;

    public ICPC_Query2(String codePath, String reqPath, String cchitPath, String hippaPath) throws Exception {
        super(codePath, reqPath, cchitPath, hippaPath);
        this.codePath = codePath;
        //String query = "authenticate,authen,credential,challenge,kerberos,auth,login,otp,cred,role,permission,user,access,resource,security,secure";
        String query = "authenticate,authen,credential,kerberos,login,otp,credential,permission,access,security,secure";
        Row query_row = RowFactory.create("1", query);
        query_data = createVistaDataset(REQ_ID, REQ_CONTENT, Arrays.asList(query_row), sparkSession);
        batch_size = 8000;
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

    public long traceQueryCode() throws Exception {
        int iterNum = codeFilePaths.size() / batch_size;
        long time_without_io = 0;
        String VSM_SCORE = "QUERY_CODE_VSM_SCORE";
        long start = System.currentTimeMillis();
        Dataset select_code = null;
        int index = 0;
        for (int i = 0; i <= iterNum; i++) {
            List<Row> rows = readCode(codeFilePaths.subList(index, Math.min(index + batch_size, codeFilePaths.size())));
            index += batch_size;
            Dataset code = createVistaDataset(CODE_ID, CODE_CONTENT, rows, sparkSession);

            // training
            long train_start = System.currentTimeMillis();
            SparkTraceTask tracer = tracerBuilder.getTask(sourceId, targetId);
            Map<String, String> config = new HashMap<>();
            config.put("s_id", CODE_ID);
            config.put("t_id", REQ_ID);
            config.put("s_text", CODE_CONTENT);
            config.put("t_text", REQ_CONTENT);
            tracer.setConfig(config);
            syncSymbolValues(tracer);
            tracer.train(code, requirement, null);
            long train_end = System.currentTimeMillis();
            Logger.getLogger("").info(String.format("code-req batch %s training time = %s", i, System.currentTimeMillis() - train_start));

            //testing
            long trace_start = System.currentTimeMillis();
            Dataset<Row> result = tracer.trace(code, query_data);
            String vsmScore = tracer.getOutputField(tracerBuilder.getOutputColName()).getFieldSymbol().getSymbolValue();
            result = result.withColumnRenamed(vsmScore, VSM_SCORE);
//            result = result.filter(column(VSM_SCORE).gt(0.05));
            result.count();
            long trace_end = System.currentTimeMillis();
            long batch_time = trace_end - train_start;
            Logger.getLogger("").info(String.format("code-req batch %s time = %s", i, trace_end - trace_start));

            if (select_code == null) {
                select_code = result;
            } else {
                select_code = select_code.union(result);
            }
            time_without_io += batch_time;
            code.unpersist();
        }
        long ws = System.currentTimeMillis();
        select_code.select(CODE_ID, VSM_SCORE).write()
                .format("csv")
                .option("header", "true").mode("overwrite")
                .save(secure_code_dir + "/query_code");
        long we = System.currentTimeMillis();
        System.out.println(String.format("write_time = %s", we - ws));
        return time_without_io;
    }

    public long traceCodeReq() throws Exception {
//        Dataset selected_code = sparkSession.read().option("parserLib", "univocity")
//                .option("multiLine", "true").option("header", "true").csv(secure_code_dir + "/query_code/");
        Set<String> code_ids = readCodeIdFromCSV(secure_code_dir + "/query_code/");
        System.out.println(String.format("total code:%s", code_ids.size()));
        List<Path> selectedCodePaths = Files.walk(Paths.get(codePath)).filter(x -> code_ids.contains(x.getFileName().toString())).collect(Collectors.toList());
        List<Row> rows = readCode(selectedCodePaths);
        Dataset<Row> selected_code = createVistaDataset(CODE_ID, CODE_CONTENT, rows, sparkSession);

        long start = System.currentTimeMillis();
        SparkTraceTask tracer = tracerBuilder.getTask(sourceId, targetId);
        Dataset<Row> result = trace(tracer, CODE_ID, CODE_CONTENT, REQ_ID, REQ_CONTENT, selected_code, requirement);
        String vsmScore = tracer.getOutputField(tracerBuilder.getOutputColName()).getFieldSymbol().getSymbolValue();
//        result = result.filter(col(vsmScore).gt(0.05));
        WindowSpec wind = Window.partitionBy(col(CODE_ID)).orderBy(desc(vsmScore));
        result = result.filter(col(vsmScore).gt(0).and(not(col(vsmScore).isNaN()))).withColumn("rank", rank().over(wind)).where(col("rank").lt(10));
        long end = System.currentTimeMillis();
        result.select(CODE_ID, REQ_ID, vsmScore).write()
                .format("csv")
                .option("header", "true").mode("overwrite")
                .save(code_req_link_dir);
        return end - start;
    }


    public static void main(String[] args) throws Exception {
        String codePath = "G:\\download\\VistA-M-master\\Packages";
        String reqPath = "G:\\Download\\Vista\\Processed\\vista_requirement.csv"; // # = 1115
        String cchitPath = "G:\\Download\\Vista\\Processed\\Processed-CCHIT-NEW-For-Poirot.xml"; //# = 462
        String hippaPath = "G:\\Download\\Vista\\Processed\\11HIPAA_Goal_Model.xml"; // # = 10
        ICPC_Query2 exp = new ICPC_Query2(codePath, reqPath, cchitPath, hippaPath);
        long t1 = 0, t2 = 0;
//        t1 = exp.traceQueryCode();
        t2 = exp.traceCodeReq();
        String info = String.format("query_code:%s, code_req:%s", t1, t2);
        Files.write(Paths.get(outputDir + "/time.txt"), info.getBytes());
    }
}
