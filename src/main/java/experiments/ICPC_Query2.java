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
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

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


    public ICPC_Query2(String codePath, String reqPath, String cchitPath, String hippaPath) throws Exception {
        super(codePath, reqPath, cchitPath, hippaPath);
        String query = "authenticate,authen,credential,challenge,kerberos,auth,login,otp,cred,role,permission,user,access,resource,security,secure";

        Row query_row = RowFactory.create("1", query);
        query_data = createVistaDataset(QUERY_ID, QUERY_CONTENT, Arrays.asList(query_row), sparkSession);
        query_data.cache().count();
        batch_size = 8000;
        tracerBuilder = new VSMTraceBuilder();
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
            SparkTraceTask tracer = tracerBuilder.getTask(sourceId, targetId);
            long batch_start = System.currentTimeMillis();
            Dataset result = trace(tracer, QUERY_ID, QUERY_CONTENT, CODE_ID, CODE_CONTENT, query_data, code);
            String vsmScore = tracer.getOutputField(tracerBuilder.getOutputColName()).getFieldSymbol().getSymbolValue();

            result = result.select(CODE_ID, vsmScore).withColumnRenamed(vsmScore, VSM_SCORE);
            result = result.filter(col(VSM_SCORE).gt(0.05));

            if (select_code == null) {
                select_code = result;
            } else {
                select_code = select_code.union(result);
            }
            long batch_end = System.currentTimeMillis();
            long batch_time = batch_end - batch_start;
            Logger.getLogger("").info(String.format("code-req batch %s time = %s", i, batch_end - batch_start));
            time_without_io += batch_time;
            code.unpersist();
        }
        long end = System.currentTimeMillis();
        select_code.select(CODE_ID).write()
                .format("csv")
                .option("header", "true").mode("overwrite")
                .save(secure_code_dir + "/query_code");
        return time_without_io;
    }

    public long traceCodeReq() throws Exception {
        Dataset selected_code = sparkSession.read().option("parserLib", "univocity")
                .option("multiLine", "true").option("header", "true").csv(secure_code_dir + "/query_code/");
        long start = System.currentTimeMillis();
        SparkTraceTask tracer = tracerBuilder.getTask(sourceId, targetId);
        tracer.indexOn = 1;
        Dataset<Row> result = trace(tracer, CODE_ID, CODE_CONTENT, REQ_ID, REQ_CONTENT, selected_code, requirement);
        String vsmScore = tracer.getOutputField(tracerBuilder.getOutputColName()).getFieldSymbol().getSymbolValue();
        WindowSpec wind = Window.partitionBy(col(CODE_ID)).orderBy(desc(vsmScore));
        result = result.filter(col(vsmScore).gt(0).and(not(col(vsmScore).isNaN()))).withColumn("rank", rank().over(wind)).where(col("rank").lt(10));
        result.select(CODE_ID, REQ_ID, vsmScore).write()
                .format("csv")
                .option("header", "true").mode("overwrite")
                .save(code_req_link_dir);
        long end = System.currentTimeMillis();
        return end - start;
    }


    public static void main(String[] args) throws Exception {
        String codePath = "F:\\download\\VistA-M-master\\Packages";
        String reqPath = "F:\\Download\\Vista\\Processed\\vista_requirement.csv"; // # = 1115
        String cchitPath = "F:\\Download\\Vista\\Processed\\Processed-CCHIT-NEW-For-Poirot.xml"; //# = 462
        String hippaPath = "F:\\Download\\Vista\\Processed\\11HIPAA_Goal_Model.xml"; // # = 10
        ICPC_Query2 exp = new ICPC_Query2(codePath, reqPath, cchitPath, hippaPath);
        long t1 = 0, t2 = 0;
        t1 = exp.traceQueryCode();
//        t2 = exp.traceCodeReq();
        String info = String.format("query_code:%s, code_req:%s", t1, t2);
        Files.write(Paths.get(outputDir + "/time.txt"), info.getBytes());
    }
}
