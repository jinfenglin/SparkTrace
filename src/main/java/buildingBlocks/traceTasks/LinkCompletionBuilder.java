package buildingBlocks.traceTasks;

import core.SparkTraceTask;
import core.graphPipeline.basic.SGraph;

/**
 * ICSE example of LinkCompletion (LC)
 */
public class LinkCompletionBuilder implements TraceTaskBuilder{
    @Override
    public SGraph createSDF() throws Exception {
        return null;
    }

    @Override
    public SGraph createDDF() throws Exception {
        return null;
    }

    @Override
    public SparkTraceTask connectTask(SparkTraceTask traceTask) throws Exception {
        return null;
    }

    @Override
    public SparkTraceTask getTask(String sourceId, String targetId) throws Exception {
        return null;
    }
}
