package core.TraceLabAdaptor.dataModel.IO;

import org.w3c.dom.Element;
import org.w3c.dom.Node;

/**
 * Data structure for  <IOItem>
 */
public class IOItem {
    public static String IO_ITEM_DEF = "IOItemDefinition", MAP_TO = "MappedTo";
    IOItemDefinition def;
    String mapTo;

    public IOItem(Node IONode) {
        Element e = (Element) IONode;
        Element IODef = (Element) e.getElementsByTagName(IO_ITEM_DEF).item(0);
        def = new IOItemDefinition(IODef);
        Node mapToNode = e.getElementsByTagName(MAP_TO).item(0);
        mapTo = mapToNode.getTextContent();
    }
}
