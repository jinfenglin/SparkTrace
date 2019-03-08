package experiments;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.io.OutputStream;

/**
 *
 */
public class Maven3ArtifactExperimentUnoptimized extends Maven3ArtifactsExperiment {
    public Maven3ArtifactExperimentUnoptimized(String codeFilePath, String bugPath, String commitPath, String sourceCodeRootDir) throws IOException {
        super(codeFilePath, bugPath, commitPath, sourceCodeRootDir);
    }

    public static void main(String[] args) throws Exception {
        String outputDir = "results"; // "results"
        String codePath = "src/main/resources/maven_sample/code.csv";
        String bugPath = "src/main/resources/maven_sample/bug.csv";
        String commitPath = "src/main/resources/maven_sample/commits.csv";
        String sourceCodeRootDir = "src/main/resources/maven_sample";
        Maven3ArtifactsExperiment exp = new Maven3ArtifactsExperiment(codePath, bugPath, commitPath, sourceCodeRootDir);
        long unOpTime = exp.runUnOptimizedSystem();
        org.apache.hadoop.fs.Path outputPath = new org.apache.hadoop.fs.Path(outputDir + "/Maven3ArtifactResultUnOp.csv");
        OutputStream out = outputPath.getFileSystem(new Configuration()).create(outputPath);
        String unOpTimeLine = "Unop Time = " + String.valueOf(unOpTime) + "\n";
        out.write(unOpTimeLine.getBytes());
        out.close();
        System.out.println(unOpTimeLine);
    }
}
