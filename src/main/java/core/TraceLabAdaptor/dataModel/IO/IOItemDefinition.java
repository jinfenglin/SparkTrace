package core.TraceLabAdaptor.dataModel.IO;

import org.w3c.dom.Element;
import org.w3c.dom.Node;

/**
 *
 */
public class IOItemDefinition {
    public static String IO_ITEM_DEF = "IOItemDefinition";
    public static String NAME = "Name", TYPE = "Type", IO_TYPE = "IOType";
    String fieldName, dataType, IOType;

    public IOItemDefinition(Node IODef) {
        Element e = (Element) IODef;
        fieldName = e.getAttribute(NAME);
        dataType = e.getAttribute(TYPE);
        IOType = e.getAttribute(IO_TYPE);
    }
}
