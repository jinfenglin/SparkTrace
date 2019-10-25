package core.TraceLabAdaptor.dataModel.IO;

import org.w3c.dom.Element;
import org.w3c.dom.Node;

/**
 * Data structure for  <IOItem>
 */
public class IOItem {
    public static String IO_ITEM_DEF = "IOItemDefinition", MAP_TO = "MappedTo";
    public static String NAME = "Name", TYPE = "Type", IO_TYPE = "IOType";
    String fieldName, dataType, IOType, mapTo;

    public IOItem(Node IONode) {
        Element e = (Element) IONode;
        Element IODef = (Element) e.getElementsByTagName(IO_ITEM_DEF).item(0);
        fieldName = IODef.getAttribute(NAME);
        dataType = IODef.getAttribute(TYPE);
        IOType = IODef.getAttribute(IO_TYPE);
        Node mapToNode = e.getElementsByTagName(MAP_TO).item(0);
        mapTo = mapToNode.getTextContent();
    }
}
