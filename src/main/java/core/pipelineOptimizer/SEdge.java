package core.pipelineOptimizer;

import core.GraphSymbol.Symbol;

import java.util.Map;

/**
 *
 */
public class SEdge {
    private Vertex from, to;
    private Map<Symbol, Symbol> connections;

    public SEdge(Vertex from, Vertex to, Map<Symbol, Symbol> connections) {
        this.from = from;
        this.to = to;
        this.connections = connections;
    }

    public Vertex getFrom() {
        return from;
    }

    public void setFrom(Vertex from) {
        this.from = from;
    }

    public Vertex getTo() {
        return to;
    }

    public void setTo(Vertex to) {
        this.to = to;
    }

    public Map<Symbol, Symbol> getConnections() {
        return connections;
    }

    public void setConnections(Map<Symbol, Symbol> connections) {
        this.connections = connections;
    }
}
