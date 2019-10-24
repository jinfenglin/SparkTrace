package core.TraceLabAdaptor;

import core.graphPipeline.FLayer.FGraph;
import core.graphPipeline.basic.Vertex;

/**
 * Parse a TCML file into FLayer vertex. TCML file is used by TraceLab to record Composite Components.
 * It is can be either:
 * 1. A Complex FGraph constituted by FGraphs and FNodes.
 * 2. A Composite Flow Node (CFNode) which are created with SNodes and SGraphs (which are also stored in TCML files)
 */
public class TCMLParser {
    public TCMLParser(String filePath) {

    }

    /**
     * @return
     */
    public Vertex parse() {
        return null;
    }
}
