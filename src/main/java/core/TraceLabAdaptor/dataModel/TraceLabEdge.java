package core.TraceLabAdaptor.dataModel;

import org.w3c.dom.Element;
import org.w3c.dom.Node;


/**
 *
 */
public class TraceLabEdge {
    public static String ID = "id", SOURCE = "source", TARGET = "target";
    String edgeId;
    String source, target;

    public TraceLabEdge(Node edgeNode) {
        Element e = (Element) edgeNode;
        edgeId = e.getAttribute(ID);
        source = e.getAttribute(SOURCE);
        target = e.getAttribute(TARGET);
    }
}
