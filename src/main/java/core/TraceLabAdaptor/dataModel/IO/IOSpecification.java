package core.TraceLabAdaptor.dataModel.IO;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.ArrayList;
import java.util.List;

/**
 * Model the IOItem block
 */
public class IOSpecification {
    static String INPUT = "Input", OUTPUT = "Output";
    static String IO_ITEM = "IOItem";
    List<IOItem> inputs, outputs;

    public IOSpecification(Node IOSpec) {
        if (IOSpec == null) {
            inputs = new ArrayList<>();
            outputs = new ArrayList<>();
        } else {
            Element e = (Element) IOSpec;
            Node inputSec = e.getElementsByTagName(INPUT).item(0);
            Node outputSec = e.getElementsByTagName(OUTPUT).item(0);
            inputs = parseSection(inputSec);
            outputs = parseSection(outputSec);
        }
    }

    /**
     * Parse <Input></Input>and <Output> </Output> section
     *
     * @return
     */
    private List<IOItem> parseSection(Node section) {
        List<IOItem> res = new ArrayList<>();
        NodeList items = section.getChildNodes();
        for (int i = 0; i < items.getLength(); i++) {
            Node itemNode = items.item(i);
            if (itemNode.getNodeName().equals(IO_ITEM)) {
                IOItem IOitem = new IOItem(itemNode);
                res.add(IOitem);
            }
        }
        return res;
    }

    public List<IOItem> getInputs() {
        return inputs;
    }

    public void setInputs(List<IOItem> inputs) {
        this.inputs = inputs;
    }

    public List<IOItem> getOutputs() {
        return outputs;
    }

    public void setOutputs(List<IOItem> outputs) {
        this.outputs = outputs;
    }
}
