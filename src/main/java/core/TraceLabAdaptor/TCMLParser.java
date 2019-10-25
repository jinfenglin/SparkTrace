package core.TraceLabAdaptor;

import core.TraceLabAdaptor.dataModel.IO.IOItemDefinition;
import core.TraceLabAdaptor.dataModel.TraceComposite;
import core.TraceLabAdaptor.dataModel.TraceLabEdge;
import core.TraceLabAdaptor.dataModel.TraceLabNode;
import core.graphPipeline.FLayer.FGraph;
import core.graphPipeline.basic.Vertex;
import core.graphPipeline.graphSymbol.Symbol;
import org.w3c.dom.Element;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static core.TraceLabAdaptor.TEMLParser.fileToDOM;

/**
 * Parse a TCML file into FLayer vertex. TCML file is used by TraceLab to record Composite Components.
 * It is can be either:
 * 1. A Complex FGraph constituted by FGraphs and FNodes.
 * 2. A Composite Flow Node (CFNode) which are created with SNodes and SGraphs (which are also stored in TCML files)
 */
public class TCMLParser {

    public static FGraph toGraph(TraceComposite tc) throws Exception {
        FGraph fg = new FGraph();
        //Update source and sink node based on the tc.inputs and outputs
        for (IOItemDefinition inputField : tc.getInputs()) {
            fg.addInputField(inputField.getFieldName());
        }
        for (IOItemDefinition outputField : tc.getOutputs()) {
            fg.addOutputField(outputField.getFieldName());
        }



        List<TraceLabNode> vertices = tc.getVertices();
        List<TraceLabEdge> edges = tc.getEdges();
        Map<String, Vertex> verticesIndex = new HashMap<>();
        for (TraceLabNode node : vertices) {

        }
        return fg;
    }


    public static void main(String[] args) throws Exception {
        Path filePath = Paths.get("src/main/resources/tracelab/wf_tfvector.tcml");
        Element root = fileToDOM(filePath);
        TraceComposite tc = new TraceComposite(root);
    }
}
