package experiments;

import core.SparkTraceTask;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Logger;

import static org.apache.spark.sql.functions.col;
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
        String query = "authenticate,credential,challenge,kerberos,auth,login,otp,cred,role,permission,user,access,resource,security,secure";

        Row query_row = RowFactory.create("1", query);
        query_data = createVistaDataset(QUERY_ID, QUERY_CONTENT, Arrays.asList(query_row), sparkSession);

    }

    public long traceQueryCode() throws Exception {
        Files.walk(Paths.get(secure_code_dir))
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
        int iterNum = codeFilePaths.size() / batch_size;
        long time_without_io = 0;
        String CODE_ID_SE = "CODE_ID_SE";
        long start = System.currentTimeMillis();
        int index = 0;
        for (int i = 0; i <= iterNum; i++) {
            long batch_start = System.currentTimeMillis();
            List<Row> rows = readCode(codeFilePaths.subList(index, Math.min(index + batch_size, codeFilePaths.size())));
            index += batch_size;
            Dataset code = createVistaDataset(CODE_ID, CODE_CONTENT, rows, sparkSession);
            SparkTraceTask tracer = tracerBuilder.getTask(sourceId, targetId);
            Dataset result = trace(tracer, QUERY_ID, QUERY_CONTENT, CODE_ID, CODE_CONTENT, query_data, code);
            String vsmScore = tracer.getOutputField(tracerBuilder.getOutputColName()).getFieldSymbol().getSymbolValue();
            result = result.drop(col(QUERY_ID)).filter(col(vsmScore).leq(0.5)).withColumnRenamed(CODE_ID, CODE_ID_SE);
            result = result.join(code, code.col(CODE_ID).equalTo(result.col(CODE_ID_SE)), "left_outer");
            result.count(); // invoke computation to compute time
            long batch_end = System.currentTimeMillis();
            result.select(CODE_ID, CODE_CONTENT).write()
                    .format("csv")
                    .option("header", "true").mode("append")
                    .save(secure_code_dir);

            long batch_time = batch_end - batch_start;
            time_without_io += batch_time;
            Logger.getLogger("").info(String.format("code-req batch %s time = %s", i, batch_end - batch_start));
            code.unpersist();
        }
        long end = System.currentTimeMillis();
        return time_without_io;
    }

    public long traceCodeReq() throws Exception {
        long start = System.currentTimeMillis();
        Dataset selected_code = sparkSession.read().option("header", "true").csv(secure_code_dir);
        trace(CODE_ID, CODE_CONTENT, REQ_ID, REQ_CONTENT, selected_code, requirement, code_req_link_dir);
        long end = System.currentTimeMillis();
        return end - start;
    }


    public static void main(String[] args) throws Exception {
        String codePath = "G:\\Download\\VistA-M-master";
        String reqPath = "G:\\Download\\Vista\\Processed\\Processed-Vista-NEW.xml"; // # = 1115
        String cchitPath = "G:\\Download\\Vista\\Processed\\Processed-CCHIT-NEW-For-Poirot.xml"; //# = 462
        String hippaPath = "G:\\Download\\Vista\\Processed\\11HIPAA_Goal_Model.xml"; // # = 10
        ICPC_Query2 exp = new ICPC_Query2(codePath, reqPath, cchitPath, hippaPath);
        long t1 = exp.traceQueryCode();
        long t2 = exp.traceCodeReq();
        String info = String.format("query_code:%s, code_req:%s", t1, t2);
        Files.write(Paths.get(outputDir + "/time.txt"), info.getBytes());
    }
}
