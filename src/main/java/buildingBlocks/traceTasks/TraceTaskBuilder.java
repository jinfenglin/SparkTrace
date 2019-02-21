package buildingBlocks.traceTasks;

import core.SparkTraceTask;
import core.graphPipeline.SDF.SDFGraph;
import core.graphPipeline.basic.SGraph;

/**
 *
 */
public interface TraceTaskBuilder {
    SDFGraph createSDF() throws Exception;

    SGraph createDDF() throws Exception;

    SparkTraceTask connectSDFToDDF(SparkTraceTask traceTask) throws Exception;

    SparkTraceTask getTask(String sourceId, String targetId) throws Exception;
}
