import core.TraceLabAdaptor.TCML.CompositeVertex.CFNodeBuilder;
import core.TraceLabAdaptor.dataModel.TraceComposite;
import core.graphPipeline.FLayer.CFNode;
import org.junit.Test;
import org.w3c.dom.Element;

import java.nio.file.Path;
import java.nio.file.Paths;

import static core.TraceLabAdaptor.TEMLParser.fileToDOM;

/**
 *
 */
public class SparkTraceVertexBuilderTest {

    private TraceComposite getTraceComposite(String path) throws Exception {
        Path filePath = Paths.get(path);
        Element root = fileToDOM(filePath);
        TraceComposite tc = new TraceComposite(root);
        return tc;
    }

    @Test
    public void CFNodeBuilderTest() throws Exception {
        TraceComposite tc = getTraceComposite("src/main/resources/tracelab/cfn_cosinesimilarity.tcml");
        CFNodeBuilder builder = new CFNodeBuilder(tc, "");
        CFNode node = builder.buildCFNode();
        int i = 0;
    }
}
