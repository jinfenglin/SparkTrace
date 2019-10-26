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

    public String getEdgeId() {
        return edgeId;
    }

    public void setEdgeId(String edgeId) {
        this.edgeId = edgeId;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }
}
