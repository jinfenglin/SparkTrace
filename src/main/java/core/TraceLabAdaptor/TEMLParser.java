package core.TraceLabAdaptor;

import core.TraceLabAdaptor.dataModel.TraceLabEdge;
import core.TraceLabAdaptor.dataModel.TraceLabNode;
import core.graphPipeline.FLayer.FGraph;
import core.graphPipeline.basic.Edge;
import core.graphPipeline.basic.Vertex;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.io.StringReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Parse a TEML file into a workflow graph.
 */
public class TEMLParser {
    public static Element fileToDOM(Path TEMLpath) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document document = builder.parse(new File(TEMLpath.toString()));
        document.getDocumentElement().normalize();
        Element root = document.getDocumentElement();
        return root;
    }

    public static Element StringToDOM(String temlStr) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        InputSource is = new InputSource(new StringReader(temlStr));
        Document document = builder.parse(is);
        document.getDocumentElement().normalize();
        Element root = document.getDocumentElement();
        return root;
    }

    public static List<TraceLabNode> parseNodes(Element root) throws Exception {
        NodeList graphBody = root.getChildNodes();
        List<TraceLabNode> vertices = new ArrayList<>();
        for (int i = 0; i < graphBody.getLength(); i++) {
            Node node = graphBody.item(i);
            if (node.getNodeType() == node.ELEMENT_NODE) {
                String tag = ((Element) node).getTagName();
                if (tag.equals("node")) {
                    vertices.add(new TraceLabNode(node));
                }
            }
        }
        return vertices;
    }

    public static List<TraceLabEdge> parseEdges(Element root) {
        NodeList graphBody = root.getChildNodes();
        List<TraceLabEdge> edges = new ArrayList<>();
        for (int i = 0; i < graphBody.getLength(); i++) {
            Node node = graphBody.item(i);
            if (node.getNodeType() == node.ELEMENT_NODE) {
                String tag = ((Element) node).getTagName();
                if (tag.equals("edge")) {
                    edges.add(new TraceLabEdge(node));
                }
            }
        }
        return edges;
    }

    /**
     * Create a FGraph and configure it properly
     *
     * @param vertices
     * @param edges
     * @return
     */
    public FGraph createFGraph(List<Vertex> vertices, List<Edge> edges) {
        return null;
    }

    /**
     * Parse a node into edge
     *
     * @return
     */
    public Vertex parserVertex(Node node) {
        return null;
    }

    public Edge parseEdge(Node node) {
        return null;
    }

    public static void main(String[] args) throws Exception {
        Path testFile = Paths.get("src/main/resources/tracelab/vsm_flow.teml");
        TEMLParser temlParser = new TEMLParser();
    }
}
