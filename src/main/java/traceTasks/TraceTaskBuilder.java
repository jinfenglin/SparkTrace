package traceTasks;

import core.SparkTraceTask;
import core.graphPipeline.SLayer.SGraph;

/**
 *
 */
public interface TraceTaskBuilder {
    SGraph createSDF() throws Exception;

    SGraph createDDF() throws Exception;

    SparkTraceTask connectTask(SparkTraceTask traceTask) throws Exception;

    SparkTraceTask getTask(String sourceId, String targetId) throws Exception;

    String getOutputColName();

    default void setDebugFlag(boolean flag) {
    }
}
