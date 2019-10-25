package core.TraceLabAdaptor.dataModel;

import core.TraceLabAdaptor.dataModel.IO.IOSpecification;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.HashMap;
import java.util.Map;

/**
 * Data structure for parsing <node></node> block in TEML and TCML files.
 */
public class TraceLabNode {
    static String SERIALIZED_VERTEX_DATA = "SerializedVertexData", META_DATA = "Metadata", TYPE = "type";
    static String META_COMPONENT = "ComponentMetadata", META_COMPOSITE = "CompositeComponentMetadata", META_START = "StartNodeMetadata", META_END = "EndNodeMetadata";
    static String CONFIG_VALUES = "ConfigValues", PROPERTY_OBJECT = "PropertyObject";
    static String IO_SPEC = "IOSpec", CONFIG_WRAP = "ConfigWrapper";
    static String ID = "id", LABEL = "Label";

    String nodeId, label;
    boolean isComposite;
    IOSpecification IOSpec;
    Map<String, NodeConfigure> config; //Configuration in hash map <name,value>

    public TraceLabNode(Node node) throws Exception {
        Element e = (Element) node;
        nodeId = e.getAttribute(ID);
        Node vertexData = e.getElementsByTagName(SERIALIZED_VERTEX_DATA).item(0);
        Element vertexDataEle = (Element) vertexData;
        Element meta = (Element) vertexDataEle.getElementsByTagName(META_DATA).item(0);
        label = meta.getAttribute(LABEL);
        isComposite = isComposite(meta);

        Node IOSpecNode = meta.getElementsByTagName(IO_SPEC).item(0);
        IOSpec = new IOSpecification(IOSpecNode);

        Node configNode = meta.getElementsByTagName(CONFIG_WRAP).item(0);
        readConfig(configNode);

        int i = 0;
    }

    private void readConfig(Node configNode) {
        config = new HashMap<>();
        if (configNode == null) {
            return;
        }
        Element e = (Element) configNode;
        Node configValue = e.getElementsByTagName(CONFIG_VALUES).item(0);
        config = parseConfigProps(configValue);
    }

    public static Map<String, NodeConfigure> parseConfigProps(Node propSec) {
        NodeList properties = propSec.getChildNodes();
        Map<String, NodeConfigure> res = new HashMap<>();
        for (int i = 0; i < properties.getLength(); i++) {
            Node property = properties.item(i);
            if (property.getNodeType() == Node.ELEMENT_NODE) {
                Element propEle = (Element) property;
                if (propEle.getTagName().equals(PROPERTY_OBJECT)) {
                    NodeConfigure nodeConfigure = new NodeConfigure(propEle);
                    res.put(nodeConfigure.name, nodeConfigure);
                }
            }
        }
        return res;
    }

    private String getComponentType(String metaType) {
        String typeInfo = metaType.split(",")[0];
        String[] parts = typeInfo.split("\\.");
        return parts[parts.length - 1];
    }

    private boolean isComposite(Element meta) throws Exception {
        String metaType = meta.getAttribute(TYPE);
        String componentType = getComponentType(metaType);
        if (componentType.equals(META_COMPOSITE)) {
            return true;
        } else if (componentType.equals(META_COMPONENT)) {
            return false;
        } else if (componentType.equals(META_START) || componentType.equals(META_END)) {
            return false;
        } else {
            throw new Exception(String.format("Unrecognized node meta type %s", componentType));
        }
    }
}
