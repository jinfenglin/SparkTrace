package core.TraceLabAdaptor.dataModel;

import core.TraceLabAdaptor.dataModel.IO.IOItemDefinition;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static core.TraceLabAdaptor.TEMLParser.parseEdges;
import static core.TraceLabAdaptor.TEMLParser.parseNodes;
import static core.TraceLabAdaptor.dataModel.TraceLabNode.parseConfigProps;

/**
 *
 */
public class TraceComposite {
    static String INFO = "Info", INPUT = "Input", OUTPUT = "Output", CONFIG = "ConfigDefinition", COMPONENT_GRAPH = "ComponentGraph";
    static String GRAPH = "graph";
    static String ID = "ID", LABEL = "Label", NAME = "Name";
    static String IO_ITEM_DEF = "IOItemDefinition";

    static String CONFIG_PROP = "ConfigProperties";

    String id, label, name;
    List<IOItemDefinition> inputs, outputs;

    Map<String, NodeConfigure> config; // composite node lvl configuration
    List<TraceLabNode> vertices;
    List<TraceLabEdge> edges;

    public TraceComposite(Node compositeNode) throws Exception {
        Element e = (Element) compositeNode;
        Node infoNode = getElementByTag(e, INFO);
        Node inputNode = getElementByTag(e, INPUT);
        Node outputNode = getElementByTag(e, OUTPUT);
        Node configNode = getElementByTag(e, CONFIG);
        Node componentGraph = getElementByTag(e, COMPONENT_GRAPH);
        Node graph = getElementByTag((Element) componentGraph, GRAPH);

        parseInfo(infoNode);
        inputs = parseIONodes(inputNode);
        outputs = parseIONodes(outputNode);
        parseConfig(configNode);
        vertices = parseNodes((Element) graph);
        edges = parseEdges((Element) graph);
    }

    private void parseInfo(Node infoNode) {
        Element e = (Element) infoNode;
        id = getElementByTag(e, ID).getTextContent();
        label = getElementByTag(e, LABEL).getTextContent();
        name = getElementByTag(e, NAME).getTextContent();
    }

    private List<IOItemDefinition> parseIONodes(Node IONode) {
        List<IOItemDefinition> res = new ArrayList<>();
        NodeList IOs = IONode.getChildNodes();
        for (int i = 0; i < IOs.getLength(); i++) {
            Node IO = IOs.item(i);
            if (IO.getNodeType() == Node.ELEMENT_NODE) {
                Element e = (Element) IO;
                if (e.getTagName().equals(IO_ITEM_DEF)) {
                    IOItemDefinition def = new IOItemDefinition(e);
                    res.add(def);
                }
            }
        }
        return res;
    }

    private void parseConfig(Node configNode) {
        Node configProps = getElementByTag((Element) configNode, CONFIG_PROP);
        config = parseConfigProps(configProps);
    }

    public static Node getElementByTag(Element e, String tag) {
        return e.getElementsByTagName(tag).item(0);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<IOItemDefinition> getInputs() {
        return inputs;
    }

    public void setInputs(List<IOItemDefinition> inputs) {
        this.inputs = inputs;
    }

    public List<IOItemDefinition> getOutputs() {
        return outputs;
    }

    public void setOutputs(List<IOItemDefinition> outputs) {
        this.outputs = outputs;
    }

    public Map<String, NodeConfigure> getConfig() {
        return config;
    }

    public void setConfig(Map<String, NodeConfigure> config) {
        this.config = config;
    }

    public List<TraceLabNode> getVertices() {
        return vertices;
    }

    public void setVertices(List<TraceLabNode> vertices) {
        this.vertices = vertices;
    }

    public List<TraceLabEdge> getEdges() {
        return edges;
    }

    public void setEdges(List<TraceLabEdge> edges) {
        this.edges = edges;
    }
}
