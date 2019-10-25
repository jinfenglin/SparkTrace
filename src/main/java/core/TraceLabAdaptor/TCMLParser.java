package core.TraceLabAdaptor;

import core.TraceLabAdaptor.dataModel.TraceComposite;
import core.graphPipeline.FLayer.FGraph;
import core.graphPipeline.basic.Vertex;
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
    public static void main(String[] args) throws Exception {
        Path filePath = Paths.get("src/main/resources/tracelab/wf_tfvector.tcml");
        Element root = fileToDOM(filePath);
        TraceComposite tc = new TraceComposite(root);
    }
}
