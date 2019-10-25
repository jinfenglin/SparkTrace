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

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Parse a TEML file into a workflow graph.
 */
public class TEMLParser {
    private String filePath;

    public TEMLParser(String filePath) throws IOException {
        this.filePath = filePath;
    }

    public FGraph parseGraph() throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document document = builder.parse(new File(filePath));
        document.getDocumentElement().normalize();

        Element root = document.getDocumentElement();
        System.out.println(root.getNodeName());
        NodeList graphBody = document.getElementsByTagName("graph").item(0).getChildNodes();
        List<TraceLabNode> vertices = new ArrayList<>();
        List<TraceLabEdge> edges = new ArrayList<>();
        for (int i = 0; i < graphBody.getLength(); i++) {
            Node node = graphBody.item(i);
            if (node.getNodeType() == node.ELEMENT_NODE) {
                String tag = ((Element) node).getTagName();
                if (tag.equals("node")) {
                    vertices.add(new TraceLabNode(node));
                } else if (tag.equals("edge")) {
                    edges.add(new TraceLabEdge(node));
                }
            }
        }
        return null;
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
        String testFile = "src/main/resources/vsm_flow.teml";
        TEMLParser temlParser = new TEMLParser(testFile);
        FGraph graph = temlParser.parseGraph();
    }
}
