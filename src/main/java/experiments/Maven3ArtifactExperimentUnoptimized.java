package experiments;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class Maven3ArtifactExperimentUnoptimized extends Maven3ArtifactsExperiment {
    public Maven3ArtifactExperimentUnoptimized(String codeFilePath, String bugPath, String commitPath, String sourceCodeRootDir) throws IOException {
        super(codeFilePath, bugPath, commitPath, sourceCodeRootDir);
    }

    public static void main(String[] args) throws Exception {
        String outputDir = "results"; // "results"
        //String dataDirRoot = "G://Document//data_csv";
        String dataDirRoot = "src/main/resources";
        List<String> projects = new ArrayList<>();
        //projects.addAll(Arrays.asList(new String[]{"derby", "drools", "groovy", "infinispan" , "maven", "pig", "seam2"}));
        projects.addAll(Arrays.asList(new String[]{"maven_sample"}));
        org.apache.hadoop.fs.Path outputPath = new org.apache.hadoop.fs.Path(outputDir + "/Maven3ArtifactResultUnOp.csv");
        OutputStream out = outputPath.getFileSystem(new Configuration()).create(outputPath);
        for (String projectPath : projects) {
            String codePath = Paths.get(dataDirRoot, projectPath, "code.csv").toString();
            String bugPath = Paths.get(dataDirRoot, projectPath, "bug.csv").toString();
            String commitPath = Paths.get(dataDirRoot, projectPath, "commits.csv").toString();
            String sourceCodeRootDir = Paths.get(dataDirRoot, projectPath).toString();
            System.out.println(projectPath);
            Maven3ArtifactsExperiment exp = new Maven3ArtifactsExperiment(codePath, bugPath, commitPath, sourceCodeRootDir);
            long unOpTime = exp.runUnOptimizedSystem();
            String unOpTimeLine = "Unop Time = " + String.valueOf(unOpTime) + "\n";
            out.write(unOpTimeLine.getBytes());
            System.out.println(unOpTimeLine);
        }
        out.close();

    }
}
