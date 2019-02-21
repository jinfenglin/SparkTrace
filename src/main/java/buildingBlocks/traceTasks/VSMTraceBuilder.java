package buildingBlocks.traceTasks;

import buildingBlocks.text2TFIDF.Text2TFIDFPipeline;
import buildingBlocks.vecSimilarityPipeline.SparseCosinSimilarityPipeline;
import core.SparkTraceTask;
import core.graphPipeline.SDF.SDFGraph;
import core.graphPipeline.basic.SGraph;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class VSMTraceBuilder implements TraceTaskBuilder {
    @Override
    public SDFGraph createSDF() throws Exception {
        SGraph graph = Text2TFIDFPipeline.getGraph("VSM_SDF");//text1,2 = "tf-idf1,2"
        Map<String, SDFGraph.SDFType> outputTypeMap = new HashMap<>();
        outputTypeMap.put("tf-idf1", SDFGraph.SDFType.SOURCE_SDF);
        outputTypeMap.put("tf-idf2", SDFGraph.SDFType.TARGET_SDF);
        SDFGraph sdfGraph = new SDFGraph(graph, outputTypeMap);
        return sdfGraph;
    }

    @Override
    public SGraph createDDF() throws Exception {
        return SparseCosinSimilarityPipeline.getGraph("VSM_DDF"); //vec1,2 - cosin_sim
    }

    @Override
    public SparkTraceTask connectSDFToDDF(SparkTraceTask task) throws Exception {
        task.connect(task.sourceNode, "s_text", task.getSdfGraph(), "text1");
        task.connect(task.sourceNode, "t_text", task.getSdfGraph(), "text2");
        task.connect(task.getSdfGraph(), "tf-idf1", task.getDdfGraph(), "vec1");
        task.connect(task.getSdfGraph(), "tf-idf2", task.getDdfGraph(), "vec2");
        task.connect(task.getDdfGraph(), "cosin_sim", task.sinkNode, "vsm_sim");
        return task;
    }

    @Override
    public SparkTraceTask getTask(String sourceId, String targetId) throws Exception {
        SparkTraceTask task = new SparkTraceTask(createSDF(), createDDF(), sourceId, targetId);
        task.setId("VSM");
        task.addInputField("s_text").addInputField("t_text");
        task.addOutputField("vsm_sim");
        connectSDFToDDF(task);
        return task;

    }
}
