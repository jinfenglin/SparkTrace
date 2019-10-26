package core.TraceLabAdaptor.TCML;

import core.TraceLabAdaptor.dataModel.IO.IOItem;
import core.TraceLabAdaptor.dataModel.IO.IOItemDefinition;
import core.TraceLabAdaptor.dataModel.TraceComposite;
import core.TraceLabAdaptor.dataModel.TraceLabEdge;
import core.TraceLabAdaptor.dataModel.TraceLabNode;
import core.TraceLabAdaptor.dataModel.TraceLabNodeUtils;
import core.graphPipeline.FLayer.CFNode;
import core.graphPipeline.FLayer.FGraph;
import core.graphPipeline.FLayer.FType;
import core.graphPipeline.SLayer.SGraph;
import core.graphPipeline.basic.Graph;
import core.graphPipeline.basic.Vertex;
import core.graphPipeline.graphSymbol.Symbol;
import org.w3c.dom.Element;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

    public static SGraph toSGraph(TraceComposite tc, String sgraphId) {
        return null;
    }

    public static FGraph toFGraph(TraceComposite tc, String fgraphId) throws Exception {
//        Graph g = new FGraph();
//        //Update source and sink node based on the tc.inputs and outputs
//        for (IOItemDefinition inputField : tc.getInputs()) {
//            g.addInputField(inputField.getFieldName());
//        }
//        for (IOItemDefinition outputField : tc.getOutputs()) {
//            g.addOutputField(outputField.getFieldName());
//        }
//
//        List<TraceLabNode> vertices = tc.getVertices();
//        List<TraceLabEdge> edges = tc.getEdges();
//        Map<String, TraceLabNode> TLNodeIndex = new HashMap<>();
//        for (TraceLabNode node : vertices) {
//            g.addNode(node.toSparkGraphVertex());
//            TLNodeIndex.put(node.getNodeId(), node);
//        }
//        for (TraceLabEdge edge : edges) {
//            Vertex sVertex = g.getNode(edge.getSource());
//            Vertex tVertex = g.getNode(edge.getTarget());
//            TraceLabNode sTLNode = TLNodeIndex.get(edge.getSource());
//            TraceLabNode tTLNode = TLNodeIndex.get(edge.getTarget());
//
//            for (IOItem sItem : sTLNode.getIOSpec().getOutputs()) {
//                for (IOItem tItem : tTLNode.getIOSpec().getInputs()) {
//                    if (sItem.isMatch(tItem)) {
//                        String mapTo = tItem.getMapTo();
//                        g.connect(sVertex, sItem.getDef().getFieldName(), tVertex, tItem.getDef().getFieldName());
//                    }
//                }
//            }
//        }
        return null;
    }


    public static void main(String[] args) throws Exception {
        Path filePath = Paths.get("src/main/resources/tracelab/wf_tfvector.tcml");
        Element root = fileToDOM(filePath);
        TraceComposite tc = new TraceComposite(root);
    }
}
