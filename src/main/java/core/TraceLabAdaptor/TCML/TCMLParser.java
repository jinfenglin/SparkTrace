package core.TraceLabAdaptor.TCML;

import core.TraceLabAdaptor.TCML.CompositeVertex.CFNodeBuilder;
import core.TraceLabAdaptor.TCML.CompositeVertex.FGraphBuilder;
import core.TraceLabAdaptor.TCML.CompositeVertex.SGraphBuilder;
import core.TraceLabAdaptor.dataModel.TraceComposite;
import core.graphPipeline.FLayer.CFNode;
import core.graphPipeline.FLayer.FGraph;
import core.graphPipeline.SLayer.SGraph;
import org.w3c.dom.Element;

import java.nio.file.Path;
import java.nio.file.Paths;

import static core.TraceLabAdaptor.TEMLParser.fileToDOM;

/**
 * Parse a TCML file into FLayer vertex. TCML file is used by TraceLab to record Composite Components.
 * It is can be either:
 * 1. A Complex FGraph constituted by FGraphs and FNodes.
 * 2. A Composite Flow Node (CFNode) which are created with SNodes and SGraphs (which are also stored in TCML files)
 */
public class TCMLParser {
    public static CFNode toCFNode(TraceComposite tc, String cfNodId) throws Exception {
        CFNodeBuilder builder = new CFNodeBuilder(tc, cfNodId);
        return builder.buildCFNode();
    }

    public static SGraph toSGraph(TraceComposite tc, String sgraphId) throws Exception {
        SGraphBuilder builder = new SGraphBuilder(tc, sgraphId);
        return builder.buildSGraph();
    }

    public static FGraph toFGraph(TraceComposite tc, String fgraphId) throws Exception {
        FGraphBuilder builder = new FGraphBuilder(tc, fgraphId);
        return builder.buildFGraph();
    }


    public static void main(String[] args) throws Exception {
        Path filePath = Paths.get("src/main/resources/tracelab/wf_tfvector.tcml");
        Element root = fileToDOM(filePath);
        TraceComposite tc = new TraceComposite(root);
    }
}
