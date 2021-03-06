package core.TraceLabAdaptor.dataModel;

import core.TraceLabAdaptor.TCML.NativeVertex.NFNodeBuilder;
import core.TraceLabAdaptor.TCML.NativeVertex.SNodeBuilder;
import core.TraceLabAdaptor.TCML.TCMLParser;
import core.TraceLabAdaptor.dataModel.IO.IOItem;
import core.graphPipeline.FLayer.CFNode;
import core.graphPipeline.FLayer.FGraph;
import core.graphPipeline.FLayer.FType;
import core.graphPipeline.SLayer.SNode;
import core.graphPipeline.basic.Vertex;
import org.w3c.dom.Element;

import java.io.FileInputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import static core.TraceLabAdaptor.TEMLParser.fileToDOM;

/**
 *
 */
public class TraceLabNodeUtils {
    /**
     * Convert TraceLab node to a SparkTrace Vertex. This is accomplish by identifying the <label> and <isComposite> of TraceLabNode.
     * 1. Composite Trace lab node are stored as TCML files and therefore we use TCML parser to convert
     * 2. Non composite on composite Node could be either Native FNode or SNode/SGraph, we use a register table to
     * record all available. The register table is a hashmap where key is the TraceLab Label and value is the SparkTrace Vertex class.
     * <p>
     * Since TraceLab is not writing the <name> of components ( which indicate node class) we have to use label. Therefore user should NOT modify the labels.
     *
     * @return
     */
    public static Vertex toSparkGraphVertex(TraceLabNode tlNode) throws Exception {
        Properties prop = new Properties();
        String fileName = "src/main/resources/sparkTrace.config";
        prop.load(new FileInputStream(fileName));
        Path componentDir = Paths.get(prop.getProperty("traceLab.dirs.tcml"));
        FType nodeType = tlNode.getComponentType();
        String vertexId = tlNode.getNodeId();
        if (tlNode.isComposite) { // A composite node is stored in TCML file.
            String label = tlNode.getLabel().toLowerCase();
            Path filePath = Paths.get(componentDir.toString(), label + ".tcml");
            TraceComposite tc = convertTCMLToTraceComposite(filePath);
            switch (tlNode.getComponentType()) {
                case CFNode:
                    return TCMLParser.toCFNode(tc, vertexId);
                case SGraph:
                    return TCMLParser.toSGraph(tc, vertexId);
                case FGraph:
                    return TCMLParser.toFGraph(tc, vertexId);
                default:
                    throw new Exception(String.format("%s should not included in a TCML file", nodeType));
            }
        } else {
            switch (tlNode.getComponentType()) {
                case SNode:
                    return SNodeBuilder.getInstance().buildSNode(tlNode,vertexId);
                case NFNode:
                    return NFNodeBuilder.getInstance().buildNFNode(tlNode, vertexId);
                default:
                    throw new Exception(String.format("%s can not be handled", nodeType));
            }
        }
    }

    public static TraceComposite convertTCMLToTraceComposite(Path filePath) throws Exception {
        Element root = fileToDOM(filePath);
        TraceComposite tc = new TraceComposite(root);
        return tc;
    }
}
