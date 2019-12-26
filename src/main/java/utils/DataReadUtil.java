package utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 *
 */
public class DataReadUtil {
    public static String CODE_ID = "code_id", CODE_CONTENT = "code_content";
    public static String REQ_ID = "req_id", REQ_CONTENT = "req_content";
    public static String CCHIT_ID = "cchit_id", CCHIT_CONTENT = "cchit_content";
    public static String HIPPA_ID = "hippa_id", HIPPA_CONTENT = "HIPPA_CONTENT";

    public static Dataset<Row> readVistaCode(String codeDirPath, SparkSession sparkSession) throws IOException {
        List<Path> filePaths = Files.walk(Paths.get(codeDirPath)).filter(Files::isRegularFile).collect(Collectors.toList());
        List<Row> rows = readCode(filePaths);
        return createVistaDataset(CODE_ID, CODE_CONTENT, rows, sparkSession);
    }

    public static List<Row> readCode(List<Path> filePaths) {
        List<Row> rows = new ArrayList<>();
        for (Path path : filePaths) {
            try {
                String content = new String(Files.readAllBytes(path));
                String fileName = path.getFileName().toString();
                rows.add(RowFactory.create(fileName, content));
            } catch (Exception e) {
                System.out.println(String.format("Skip %s for error", path.toString()));
            }
        }
        return rows;
    }

    private static List<Row> readVistaDoc(String path) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get(path));
        Pattern idPattern = Pattern.compile("<art_id>([^</]*)</art_id>");
        Pattern artTitle = Pattern.compile("<art_title>([^</]*)</art_title>");
        List<Row> rows = new ArrayList<>();
        for (String line : lines) {
            Matcher idMatcher = idPattern.matcher(line);
            Matcher artMatcher = artTitle.matcher(line);
            if (idMatcher.find() && artMatcher.find())
                rows.add(RowFactory.create(idMatcher.group(1), artMatcher.group(1)));
        }
        return rows;
    }

    public static Dataset<Row> createVistaDataset(String idCol, String contentCol, List<Row> rows, SparkSession sparkSession) {
        StructType schema = SchemaUtil.createArtifactSchema(idCol, contentCol);
        return sparkSession.createDataFrame(rows, schema);
    }

    public static Dataset<Row> readVistaReq(String reqPath, SparkSession sparkSession) throws IOException {
        List<Row> rows = readVistaDoc(reqPath);
        return createVistaDataset(REQ_ID, REQ_CONTENT, rows, sparkSession);
    }

    public static Dataset<Row> readVistaCCHIT(String cchitPath, SparkSession sparkSession) throws IOException {
        List<Row> rows = readVistaDoc(cchitPath);
        return createVistaDataset(CCHIT_ID, CCHIT_CONTENT, rows, sparkSession);
    }

    public static Dataset<Row> readVistaHIPPA(String hippaPath, SparkSession sparkSession) throws IOException {
        List<Row> rows = readVistaDoc(hippaPath);
        return createVistaDataset(HIPPA_ID, HIPPA_CONTENT, rows, sparkSession);
    }
}
