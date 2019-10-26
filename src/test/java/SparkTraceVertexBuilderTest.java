import core.TraceLabAdaptor.TCML.TCMLParser;
import core.TraceLabAdaptor.dataModel.TraceComposite;
import core.graphPipeline.FLayer.CFNode;
import core.graphPipeline.FLayer.FGraph;
import core.graphPipeline.SLayer.SGraph;
import org.junit.Assert;
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
    public void CFNodeBuilderCosinTest() throws Exception {
        TraceComposite tc = getTraceComposite("src/main/resources/tracelab/cfn_cosinesimilarity.tcml");
        CFNode node = TCMLParser.toCFNode(tc, "cfnode_test");
        Assert.assertEquals(node.getsGraph().getNodes().size(), 3);
        Assert.assertEquals(node.getsGraph().getEdges().size(), 3);
    }

    @Test
    public void SGBuilderTest() throws Exception {
        TraceComposite tc = getTraceComposite("src/main/resources/tracelab/sg_preprocess.tcml");
        SGraph sg = TCMLParser.toSGraph(tc, "sg_graph");
        Assert.assertEquals(sg.getNodes().size(), 5);
        Assert.assertEquals(sg.getEdges().size(), 4);
    }

    @Test
    public void CFNodeBuilderPreprocessTest() throws Exception {
        TraceComposite tc = getTraceComposite("src/main/resources/tracelab/cfn_preprocess.tcml");
        CFNode node = TCMLParser.toCFNode(tc, "cfnode_test");
        Assert.assertEquals(node.getsGraph().getNodes().size(), 3);
        Assert.assertEquals(node.getsGraph().getEdges().size(), 3);
    }

    @Test
    public void FGBuilderTest() throws Exception {
        TraceComposite tc = getTraceComposite("src/main/resources/tracelab/fg_vsm.tcml");
        FGraph fg = TCMLParser.toFGraph(tc, "fg_graph");
        fg.showGraph("vsm_flowgraph");
    }
}
