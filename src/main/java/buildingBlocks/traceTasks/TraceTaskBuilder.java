package buildingBlocks.traceTasks;

import core.SparkTraceTask;
import core.graphPipeline.basic.SGraph;

/**
 *
 */
public interface TraceTaskBuilder {
    SGraph createSDF() throws Exception;

    SGraph createDDF() throws Exception;

    SparkTraceTask connectSDFToDDF(SparkTraceTask traceTask) throws Exception;

    SparkTraceTask getTask(String sourceId, String targetId) throws Exception;
}
