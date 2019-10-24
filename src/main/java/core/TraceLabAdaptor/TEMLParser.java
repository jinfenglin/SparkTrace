package core.TraceLabAdaptor;

import core.graphPipeline.FLayer.FGraph;
import core.graphPipeline.basic.Edge;
import core.graphPipeline.basic.Vertex;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
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

    public FGraph parseGraph() throws ParserConfigurationException, IOException, SAXException {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document document = builder.parse(new File(filePath));
        document.getDocumentElement().normalize();
        FGraph flowGraph = new FGraph();

        //Here comes the root node
        Element root = document.getDocumentElement();
        System.out.println(root.getNodeName());
        NodeList graphBody = document.getElementsByTagName("graph").item(0).getChildNodes();
        List<Node> vertices = new ArrayList<>();
        List<Node> edges = new ArrayList<>();
        for (int i = 0; i < graphBody.getLength(); i++) {
            Node node = graphBody.item(i);
            String tag = ((Element) node).getTagName();
            if (tag.equals("node")) {
                vertices.add(node);
            } else if (tag.equals("edge")) {
                edges.add(node);
            }
        }
        return flowGraph;
    }

    /**
     * Parse a node into edge
     * @return
     */
    public Vertex parserVertex() {

    }

    public Edge parseEdge() {

    }

    public static void main(String[] args) throws IOException, ParserConfigurationException, SAXException {
        TEMLParser temlParser = new TEMLParser("C:\\Program Files (x86)\\COEST\\TraceLab\\workspace\\vsm_flow.teml");
        temlParser.parseGraph();
    }
}
