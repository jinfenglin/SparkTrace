package core.TraceLabAdaptor.dataModel;

import org.w3c.dom.Element;
import org.w3c.dom.Node;


/**
 * name can be different
 */
public class NodeConfigure {
    static String DISPLAY_NAME = "DisplayName", NAME = "Name", VALUE_TYPE = "ValueType", VALUE = "Value";
    String name, displayName, value, type;

    public NodeConfigure(Node property) {
        Element propEle = (Element) property;
        displayName = propEle.getElementsByTagName(DISPLAY_NAME).item(0).getTextContent();
        name = propEle.getElementsByTagName(NAME).item(0).getTextContent();
        value = parseValue(propEle.getElementsByTagName(VALUE).item(0).getTextContent());
        type = parseValueType(propEle.getElementsByTagName(VALUE_TYPE).item(0).getTextContent());

    }



    private String parseValue(String value) {
        return value.trim();
    }

    private String parseValueType(String type) {
        return type.split(",")[0];
    }
}
