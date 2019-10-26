package core.graphPipeline.basic;

import core.graphPipeline.SLayer.TransparentSNode;
import core.graphPipeline.graphSymbol.Symbol;
import core.graphPipeline.graphSymbol.SymbolTable;
import guru.nidi.graphviz.attribute.Label;
import guru.nidi.graphviz.engine.Format;
import guru.nidi.graphviz.engine.Graphviz;
import guru.nidi.graphviz.model.MutableGraph;
import guru.nidi.graphviz.model.MutableNode;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

import static core.pipelineOptimizer.PipelineOptimizer.*;
import static guru.nidi.graphviz.model.Factory.mutGraph;
import static guru.nidi.graphviz.model.Factory.mutNode;

/**
 *
 */
public class Graph extends Vertex {
    protected Map<String, Vertex> nodes;
    protected Set<Edge> edges; //Record the node level connection, the field level connection is recorded by the IOTable
    public Node sourceNode, sinkNode;
    protected Map<String, String> config;

    public Graph(String graphId) {
        super(graphId);
    }

    public Graph() {
        super();
    }

    public void setConfig(Map<String, String> symbolValueMap) {
        this.config = symbolValueMap;
        for (String symbolName : this.config.keySet()) {
            Symbol symbol = new Symbol(this, symbolName);
            SymbolTable.setSymbolValue(symbol, this.config.get(symbolName));
        }
    }

    /**
     * Modify the inputIOTable be synchronized to sourceNode
     *
     * @param symbol
     * @return
     * @throws Exception
     */
    @Override
    public Vertex addInputField(Symbol symbol) throws Exception {
        super.addInputField(symbol);
        Symbol addedSymbol = new Symbol(sourceNode, symbol.getSymbolName());
        sourceNode.addOutputField(addedSymbol);
        return this;
    }

    public void removeInputField(Symbol symbol) {
        super.removeInputField(symbol);
        sourceNode.removeOutputField(new Symbol(sourceNode, symbol.getSymbolName()));
    }

    @Override
    public Vertex addOutputField(Symbol symbol) throws Exception {
        super.addOutputField(symbol);
        Symbol addedSymbol = new Symbol(sinkNode, symbol.getSymbolName());
        sinkNode.addInputField(addedSymbol);
        return this;
    }

    public void removeOutputField(Symbol symbol) {
        super.removeOutputField(symbol);
        sinkNode.removeInputField(new Symbol(sinkNode, symbol.getSymbolName()));
    }

    /**
     * Let the connected symbols share same value. The final output table's column name can be renamed by a renaming stage
     */
    public static void syncSymbolValues(Graph graph) throws Exception {
        //SourceNode and SinkNode are special in the graph. The sourceNode don't have connection on its InputTable,
        //Instead, it consume graph's InputTable directly. However, the consumption is through parameters thus no explicit
        //connections are specified between graph.inputTable and sourceNode.outputTable (And inputTable should receive and have no out going links)
        for (IOTableCell graphInputCell : graph.getInputTable().getCells()) {
            Symbol graphProviderSymbol = graphInputCell.getFieldSymbol();
            Symbol sourceNodeReceiverSymbol = graph.sourceNode.getOutputTable().getSymbolByVarName(graphProviderSymbol.getSymbolName());
            SymbolTable.shareSymbolValue(graphProviderSymbol, sourceNodeReceiverSymbol);
        }

        List<TransparentSNode> transparentVertices = new ArrayList<>(); //collect transVertices for second round process
        //Need topological order of processing if we don't pick out these nodes for second round processing.
        for (Vertex node : graph.getNodes()) {
            if (node instanceof Graph) {
                syncSymbolValues((Graph) node);
            }
            for (IOTableCell providerCell : node.getOutputTable().getCells()) {
                Symbol providerSymbol = providerCell.getFieldSymbol();
                for (IOTableCell receiverCell : providerCell.getOutputTarget()) {
                    Symbol receiverSymbol = receiverCell.getFieldSymbol();
                    SymbolTable.shareSymbolValue(providerSymbol, receiverSymbol);
                    if (receiverCell.getParentTable().getContext() instanceof TransparentSNode) {
                        //Config transvertex input output symbols
                        TransparentSNode transVertex = ((TransparentSNode) receiverCell.getParentTable().getContext());
                        transVertex.matchInputToOutput();
                        transparentVertices.add(transVertex);
                    }
                }
            }
        }
        //ensure the target cell receive correct transVertex symbols
        for (TransparentSNode tv : transparentVertices) {
            for (IOTableCell providerCell : tv.getOutputTable().getCells()) {
                for (IOTableCell receiverCell : providerCell.getOutputTarget()) {
                    Symbol receiverSymbol = receiverCell.getFieldSymbol();
                    SymbolTable.shareSymbolValue(providerCell.getFieldSymbol(), receiverSymbol);
                }
            }
        }
        for (IOTableCell graphOutputCell : graph.getOutputTable().getCells()) {
            Symbol graphReceiverSymbol = graphOutputCell.getFieldSymbol();
            Symbol sinkNodeProviderSymbol = graph.sinkNode.getInputTable().getSymbolByVarName(graphReceiverSymbol.getSymbolName());
            SymbolTable.shareSymbolValue(sinkNodeProviderSymbol, graphReceiverSymbol);
        }
    }


    /**
     * remove node and its connections towards other nodes
     *
     * @param node
     */
    public void removeNode(Vertex node) throws Exception {
        if (node == null)
            return;
        if (this.nodes.containsKey(node.getVertexId())) {
            this.nodes.remove(node.getVertexId());
            node.setContext(null);
            for (Vertex fromNode : node.getInputVertices()) {
                clearConnection(fromNode, node);
            }
            for (Vertex toNode : node.getOutputVertices()) {
                clearConnection(node, toNode);
            }
        } else {
            Logger.getLogger(this.getClass().getName()).warning(String.format("%s is not contained in the graph nodes", node.getVertexId()));
        }
    }

    /**
     * Change the ID for a node within the graph. This function
     * 1. change vertex id
     * 2. change the key in node registration
     *
     * @param node
     * @param newId
     */
    public void updateNodeId(Vertex node, String newId) {
        if (node == null)
            return;
        String oldId = node.getVertexId();
        if (this.nodes.containsKey(oldId)) {
            node.setVertexId(newId);
            this.nodes.remove(oldId);
            this.nodes.put(newId, node);
        }
    }

    /**
     * Clear all connection between two vertices
     *
     * @param from
     * @param to
     */
    private void clearConnection(Vertex from, Vertex to) throws Exception {
        for (IOTableCell sourceCell : from.getOutputTable().getCells()) {
            List<IOTableCell> toVertexReceiverCells = new ArrayList<>();
            for (IOTableCell receiverCell : sourceCell.getOutputTarget()) {
                if (receiverCell.getParentTable().getContext().equals(to)) {
                    toVertexReceiverCells.add(receiverCell);
                }
            }
            for (IOTableCell receiverCell : toVertexReceiverCells) {
                disconnect(from, sourceCell.getFieldSymbol().getSymbolName(), to, receiverCell.getFieldSymbol().getSymbolName());
            }
        }
    }

    /**
     * @return
     */
    public Map<String, Integer> getDemandTable() {
        Map<String, Integer> demandTable = new HashMap<>();
        for (Vertex vertex : getNodes()) {
            for (IOTableCell cell : vertex.getOutputTable().getCells()) {
                String cellSymbolValue = cell.getFieldSymbol().getSymbolValue();
                demandTable.put(cellSymbolValue, demandTable.getOrDefault(cellSymbolValue, 0) + cell.getOutputTarget().size());
            }
        }
        return demandTable;
    }

    public List<Vertex> getNodes() {
        return new ArrayList<>(nodes.values());
    }

    public List<Edge> getEdges() {
        return new ArrayList<>(edges);
    }

    public void disconnect(Vertex v1, String symbolName1, Vertex v2, String symbolName2) throws Exception {
        Symbol s1 = new Symbol(v1, symbolName1);
        Symbol s2 = new Symbol(v2, symbolName2);
        disconnect(s1, s2);
    }

    public void disconnect(Symbol from, Symbol to) throws Exception {
        Edge edge = new Edge(from.getScope(), to.getScope());
        disconnectSymbol(from, to);
        if (noConnectionBetweenVertex(from.getScope(), to.getScope())) {
            removeEdge(edge);
        }
    }

    private void disconnectSymbol(Symbol from, Symbol to) throws Exception {
        IOTableCell fromCell = from.getScope().getOutputTable().getCellBySymbol(from);
        IOTableCell toCell = to.getScope().getInputTable().getCellBySymbol(to);
        if (fromCell != null && toCell != null) {
            fromCell.removeOutputTo(toCell);
        } else {
            throw new Exception(String.format(" %s not found", getMissingSymbolInfo(from, to)));
        }
    }

    private boolean noConnectionBetweenVertex(Vertex v1, Vertex v2) {
        boolean noConnectionLeft = true;
        for (IOTableCell cell : v1.getOutputTable().getCells()) {
            for (IOTableCell targetCell : cell.getOutputTarget()) {
                if (targetCell.getParentTable().getContext().equals(v2)) {
                    noConnectionLeft = false;
                    break;
                }
            }
        }
        return noConnectionLeft;
    }

    private String getMissingSymbolInfo(Symbol s1, Symbol s2) {
        StringJoiner notFoundInfo = new StringJoiner(",");
        IOTableCell c1 = s1.getScope().getOutputTable().getCellBySymbol(s1);
        IOTableCell c2 = s2.getScope().getInputTable().getCellBySymbol(s2);
        if (c1 == null) {
            notFoundInfo.add(s1.toString());
        }
        if (c2 == null) {
            notFoundInfo.add(s2.toString());
        }
        return notFoundInfo.toString();
    }


    public void addNode(Vertex node) throws Exception{
        nodes.put(node.getVertexId(), node);
        node.setContext(this);
    }

    public void addEdge(Edge edge) {
        edges.add(edge);
    }


    public void removeEdge(Edge edge) {
        edges.remove(edge);
    }

    public void connect(Vertex v1, String symbolName1, Vertex v2, String symbolName2) throws Exception {
        Symbol s1 = new Symbol(v1, symbolName1);
        Symbol s2 = new Symbol(v2, symbolName2);
        connect(s1, s2);
    }

    public void connect(Symbol from, Symbol to) throws Exception {
        Edge edge = new Edge(from.getScope(), to.getScope());
        addEdge(edge);
        connectSymbol(from, to);
    }

    /**
     * Connect the parent node of symbol from with parent node of to. It will update the edge sets as well.
     *
     * @param from
     * @param to
     */
    private void connectSymbol(Symbol from, Symbol to) throws Exception {
        IOTableCell fromCell = from.getScope().getOutputTable().getCellBySymbol(from);
        IOTableCell toCell = to.getScope().getInputTable().getCellBySymbol(to);
        if (fromCell != null && toCell != null) {
            fromCell.sendOutputTo(toCell);
        } else {
            throw new Exception(String.format("%s not found", (getMissingSymbolInfo(from, to))));
            //Logger.getLogger(this.getClass().getName()).info(String.format("Symbol %s to Symbol %s can not be connected", from, to));
        }
    }

    public boolean containsNode(Vertex vertex) {
        return nodes.containsKey(vertex.getVertexId());
    }


    public MutableGraph getVizGraph() {
        String vertexId = getVertexId();
        MutableGraph g = mutGraph(vertexId).setDirected(true).setCluster(true);
        g.graphAttrs().add(Label.of(getVertexLabel()));
        for (Vertex vertex : this.getNodes()) {
            if (vertex instanceof Node) {
                Node v = (Node) vertex;
                String nodeTitle = v.getVertexLabel();
                MutableNode vNode = mutNode(v.getVertexId()).add(Label.of(nodeTitle));
                for (Vertex outputNode : v.getOutputVertices()) { //note: sink node have no outputVertices, parent graph hold this information
                    vNode = createVizEdge(vNode, outputNode);
                }
                g.add(vNode);
            } else {
                Graph v = (Graph) vertex;
                MutableGraph subGraph = v.getVizGraph();
                Vertex sinkNode = v.sinkNode;
                MutableNode innerSink = mutNode(sinkNode.getVertexId()).add(Label.of("SinkNode"));
                for (Vertex outputNode : vertex.getOutputVertices()) {
                    innerSink = createVizEdge(innerSink, outputNode);
                }
                g.add(innerSink, subGraph);
            }
        }
        return g;
    }

    private MutableNode createVizEdge(MutableNode vizFromNode, Vertex outputNode) {
        if (outputNode instanceof Node) {
            vizFromNode = vizFromNode.addLink(outputNode.getVertexId());
        } else {
            Graph subGraph = (Graph) outputNode;
            Vertex innerSource = subGraph.sourceNode;
            MutableGraph innerGraph = subGraph.getVizGraph();// create inner graph, this graph is implicitly added to the parent
            vizFromNode = vizFromNode.addLink(innerSource.getVertexId());
        }
        return vizFromNode;
    }

    public void showGraph(String figName) throws IOException {
        Graphviz.fromGraph(getVizGraph()).render(Format.PNG).toFile(new File(String.format("figures/%s.png", figName)));
    }

    public Vertex getNode(String vertexId) {
        return nodes.get(vertexId);
    }


    public static List<Vertex> topologicalSort(Graph graph) throws Exception {
        List<Vertex> nodes = new ArrayList<>(graph.getNodes());
        Map<Vertex, List<Vertex>> edgeIndex = buildEdgeIndex(graph, true);
        Map<Vertex, Integer> inDegrees = inDegreeMap(graph);
        List<Vertex> sortedNodes = new ArrayList<>();
        int cnt = 0;
        //Pick the init 0 indegree vertex into sort queue
        Queue<Vertex> sortQueue = new LinkedList<>();
        for (Vertex vertex : nodes) {
            if (inDegrees.get(vertex) == 0) {
                sortQueue.add(vertex);
            }
        }
        while (sortQueue.size() > 0) {
            Vertex curVertex = sortQueue.poll();
            sortedNodes.add(curVertex);
            for (Vertex adjVertex : edgeIndex.get(curVertex)) {
                inDegrees.put(adjVertex, inDegrees.get(adjVertex) - 1);
                if (inDegrees.get(adjVertex) == 0) {
                    sortQueue.add(adjVertex);
                }
            }
            cnt += 1;
        }
        if (cnt != nodes.size()) {
            throw new Exception("Cycle found in graph when do topological sort");
        }
        return sortedNodes;
    }

    /**
     * Collect the connected verteics for each vertex in the graph
     *
     * @param graph
     * @param useFromAsIndex
     * @return
     */
    private static Map<Vertex, List<Vertex>> buildEdgeIndex(Graph graph, boolean useFromAsIndex) {
        List<Edge> edges = graph.getEdges();
        List<Vertex> vertices = graph.getNodes();
        Map<Vertex, List<Vertex>> indexMap = new HashMap<>();
        for (Vertex vertex : vertices) {
            indexMap.put(vertex, new ArrayList<>());
        }
        for (Edge edge : edges) {
            Vertex key = null;
            Vertex value = null;
            if (useFromAsIndex) { //Build edge index using the `from` node as key
                key = edge.getFrom();
                value = edge.getTo();
            } else { //Build edge index using the `to` node as key
                key = edge.getTo();
                value = edge.getFrom();
            }
            List<Vertex> connectedNodes = indexMap.get(key);
            connectedNodes.add(value);
            indexMap.put(key, connectedNodes);
        }
        return indexMap;
    }

    private static Map<Vertex, Integer> inDegreeMap(Graph graph) {
        List<Vertex> nodes = graph.getNodes();
        List<Edge> edges = graph.getEdges();
        Map<Vertex, Integer> inDegreeMap = new HashMap<>();
        for (Vertex node : nodes) {
            inDegreeMap.put(node, 0);
        }
        for (Edge edge : edges) {
            Vertex to = edge.getTo();
            if (to == null || inDegreeMap.get(to) == null) {
                int i = 0;
            }
            inDegreeMap.put(to, inDegreeMap.get(to) + 1);
        }
        return inDegreeMap;
    }

    public void optimize(Graph graph) throws Exception {
        removeDuplicatedNodes(graph);
        while (true) {
            removeRedundantInputFields(graph);
            removeRedundantOutputFields(graph);
            int removed = removeRedundantVertices(graph);
            removeEmptyGraph(graph);
            if (removed == 0) {
                break;
            }
        }
    }

}
