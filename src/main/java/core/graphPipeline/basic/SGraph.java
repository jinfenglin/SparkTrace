package core.graphPipeline.basic;

import core.graphPipeline.graphSymbol.Symbol;
import core.graphPipeline.graphSymbol.SymbolTable;
import featurePipeline.SGraphColumnRemovalStage;
import featurePipeline.SGraphIOStage;
import guru.nidi.graphviz.attribute.Label;
import guru.nidi.graphviz.engine.Format;
import guru.nidi.graphviz.engine.Graphviz;
import guru.nidi.graphviz.model.*;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

import static core.pipelineOptimizer.PipelineOptimizer.*;
import static guru.nidi.graphviz.model.Factory.*;

/**
 * A DAG which will be applied to optimize the pipeline structure.
 * To create a graph:
 * 1. Define the IO table for the graph by setting the inputTable and outputTable
 * 2. Add node to the graphs
 * 3. connectSymbol the graphs, and remember to connectSymbol to the sourceNode and sinkNode
 * 4. Add penetrations to the graph
 * 5. Call toPipeline to create a fully configured and runnable pipeline
 */
public class SGraph extends Vertex {
    private Map<String, Vertex> nodes;
    private Set<SEdge> edges; //Record the node level connection, the field level connection is recorded by the IOTable
    public SNode sourceNode, sinkNode;
    private Map<String, String> config;

    public SGraph() {
        super();
        nodes = new HashMap<>();
        edges = new HashSet<>();
        sourceNode = new SNode(new SGraphIOStage());
        sinkNode = new SNode(new SGraphIOStage());
        sourceNode.setId(String.format("%s-%s", "SourceNode", sourceNode.vertexId));
        sinkNode.setId(String.format("%s-%s", "SinkNode", sinkNode.vertexId));
        sourceNode.setContext(this);
        sinkNode.setContext(this);
        nodes.put(sourceNode.vertexId, sourceNode);
        nodes.put(sinkNode.vertexId, sinkNode);
        config = new HashMap<>();
    }

    public SGraph(String graphId) {
        super(graphId);
        nodes = new HashMap<>();
        edges = new HashSet<>();
        sourceNode = new SNode(new SGraphIOStage());
        sinkNode = new SNode(new SGraphIOStage());
        sourceNode.setId(String.format("%s-%s", "SourceNode", sourceNode.vertexId));
        sinkNode.setId(String.format("%s-%s", "SinkNode", sinkNode.vertexId));
        sourceNode.setContext(this);
        sinkNode.setContext(this);
        nodes.put(sourceNode.vertexId, sourceNode);
        nodes.put(sinkNode.vertexId, sinkNode);
        config = new HashMap<>();
    }

    public void setConfig(Map<String, String> symbolValueMap) {
        this.config = symbolValueMap;
        for (String symbolName : this.config.keySet()) {
            Symbol symbol = getInputTable().getSymbolByVarName(symbolName);
            SymbolTable.setInputSymbolValue(symbol, this.config.get(symbolName));
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
     * Let the connected symbols share same value
     */
    private static void syncSymbolValues(SGraph graph) throws Exception {
        //SourceNode and SinkNode are special in the graph. The sourceNode don't have connection on its InputTable,
        //Instead, it consume graph's InputTable directly. However, the consumption is through parameters thus no explicit
        //connections are specified between graph.inputTable and sourceNode.outputTable (And inputTable should receive and have no out going links)
        for (IOTableCell graphInputCell : graph.getInputTable().getCells()) {
            Symbol graphProviderSymbol = graphInputCell.getFieldSymbol();
            Symbol sourceNodeReceiverSymbol = graph.sourceNode.getOutputTable().getSymbolByVarName(graphProviderSymbol.getSymbolName());
            SymbolTable.shareSymbolValue(graphProviderSymbol, sourceNodeReceiverSymbol, false);
        }

        List<TransparentSNode> transparentVertices = new ArrayList<>(); //collect transVertices for second round process
        //Need topological order of processing if we don't pick out these nodes for second round processing.
        for (Vertex node : graph.getNodes()) {
            if (node instanceof SGraph) {
                syncSymbolValues((SGraph) node);
            }
            for (IOTableCell providerCell : node.outputTable.getCells()) {
                Symbol providerSymbol = providerCell.getFieldSymbol();
                for (IOTableCell receiverCell : providerCell.getOutputTarget()) {
                    Symbol receiverSymbol = receiverCell.getFieldSymbol();
                    SymbolTable.shareSymbolValue(providerSymbol, receiverSymbol, true);
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
            for (IOTableCell providerCell : tv.outputTable.getCells()) {
                for (IOTableCell receiverCell : providerCell.getOutputTarget()) {
                    Symbol receiverSymbol = receiverCell.getFieldSymbol();
                    SymbolTable.shareSymbolValue(providerCell.getFieldSymbol(), receiverSymbol, true);
                }
            }
        }

        for (IOTableCell graphOutputCell : graph.getOutputTable().getCells()) {
            Symbol graphReceiverSymbol = graphOutputCell.getFieldSymbol();
            Symbol sinkNodeProviderSymbol = graph.sinkNode.getInputTable().getSymbolByVarName(graphReceiverSymbol.getSymbolName());
            SymbolTable.shareSymbolValue(sinkNodeProviderSymbol, graphReceiverSymbol, false);
        }
    }


    /**
     * @return
     */
    public Map<IOTableCell, Integer> getDemandTable() {
        Map<IOTableCell, Integer> demandTable = new HashMap<>();
        for (Vertex vertex : getNodes()) {
            for (IOTableCell cell : vertex.getOutputTable().getCells()) {
                demandTable.put(cell, demandTable.getOrDefault(cell, 0) + cell.getOutputTarget().size());
            }
        }
        return demandTable;
    }

    /**
     * remove node and its connections towards other nodes
     *
     * @param node
     */
    public void removeNode(Vertex node) {
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


    public void optimize(SGraph graph) throws Exception {
        removeDuplicatedNodes(graph);
        while (true) {
            removeRedundantFields(graph);
            int removed = removeRedundantVertices(graph);
            removeEmptyGraph(graph);
            if (removed == 0) {
                break;
            }
        }
    }

    @Override
    public Pipeline toPipeline() throws Exception {
        boolean cleanColumns = true;
        syncSymbolValues(this);

        //Config the SGraphIOStage to parse the InputTable which translate the Symbols to real column names
        SGraphIOStage initStage = (SGraphIOStage) sourceNode.getSparkPipelineStage();
        initStage.setInputCols(inputTable);
        initStage.setOutputCols(inputTable);//IOTable consumed by inner method mapIOTableToIOParam(), the graph's inputTabel will be used

        //Config the SGraphSinkStage
        SGraphIOStage outStage = (SGraphIOStage) sinkNode.getSparkPipelineStage();
        outStage.setInputCols(outputTable);
        outStage.setOutputCols(outputTable);

        //Add Stages and create column cleaning stages in fly
        List<Vertex> topSortNodes = topologicalSort(this);
        Pipeline pipeline = new Pipeline(getVertexId());
        List<PipelineStage> stages = new ArrayList<>();
        Map<IOTableCell, Integer> demandTable = getDemandTable();
        for (Vertex node : topSortNodes) {
            stages.add(node.toPipeline());
            if (!node.equals(sinkNode) && cleanColumns) {
                for (IOTableCell targetCell : node.getInputTable().getCells()) {
                    for (IOTableCell sourceCell : targetCell.getInputSource()) {
                        if (sourceCell.getParentTable().getContext().equals(sourceNode) && this.getContext() != null) {
                            continue; //Source node contains the fields that belong to parent graph
                        }
                        int remainDemand = demandTable.get(sourceCell) - 1;
                        demandTable.put(sourceCell, remainDemand);
                        if (remainDemand == 0 && sourceCell.isRemovable()) {
                            SGraphColumnRemovalStage removalStage = new SGraphColumnRemovalStage();
                            removalStage.setInputCols(new String[]{sourceCell.getFieldSymbol().getSymbolValue()});
                            stages.add(removalStage);
                        }
                    }
                }
            }
        }
        pipeline.setStages(stages.toArray(new PipelineStage[0]));
        return pipeline;
    }

    /**
     * Collect the connected verteics for each vertex in the graph
     *
     * @param graph
     * @param useFromAsIndex
     * @return
     */
    private static Map<Vertex, List<Vertex>> buildEdgeIndex(SGraph graph, boolean useFromAsIndex) {
        List<SEdge> edges = graph.getEdges();
        List<Vertex> vertices = graph.getNodes();
        Map<Vertex, List<Vertex>> indexMap = new HashMap<>();
        for (Vertex vertex : vertices) {
            indexMap.put(vertex, new ArrayList<>());
        }
        for (SEdge edge : edges) {
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

    private static Map<Vertex, Integer> inDegreeMap(SGraph graph) {
        List<Vertex> nodes = graph.getNodes();
        List<SEdge> edges = graph.getEdges();
        Map<Vertex, Integer> inDegreeMap = new HashMap<>();
        for (Vertex node : nodes) {
            inDegreeMap.put(node, 0);
        }
        for (SEdge edge : edges) {
            Vertex to = edge.getTo();
            if (to == null) {
                int i = 0;
            }
            inDegreeMap.put(to, inDegreeMap.get(to) + 1);


        }
        return inDegreeMap;
    }

    public static List<Vertex> topologicalSort(SGraph graph) throws Exception {
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
        //DEBUG
        if (sortedNodes.size() == 4) {
            if (sortedNodes.get(2).getVertexId().equals("VSMGraph")) {
                Vertex v = sortedNodes.remove(2);
                sortedNodes.add(1, v);
            }
        }
        return sortedNodes;
    }


    public List<Vertex> getNodes() {
        return new ArrayList<>(nodes.values());
    }

    public Vertex getNode(String vertexId) {
        return nodes.get(vertexId);
    }

    public List<SEdge> getEdges() {
        return new ArrayList<>(edges);
    }

    public void addNode(Vertex node) {
        nodes.put(node.getVertexId(), node);
        node.setContext(this);
    }

    public void addEdge(SEdge edge) {
        edges.add(edge);
    }

    /**
     * Clear all connection between two vertices
     *
     * @param from
     * @param to
     */
    private void clearConnection(Vertex from, Vertex to) {
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

    public void removeEdge(SEdge edge) {
        edges.remove(edge);
    }

    public void connectSymbol(Vertex v1, String symbolName1, Vertex v2, String symbolName2) {
        Symbol s1 = new Symbol(v1, symbolName1);
        Symbol s2 = new Symbol(v2, symbolName2);
        connect(s1, s2);
    }

    public void connect(Symbol from, Symbol to) {
        SEdge edge = new SEdge(from.getScope(), to.getScope());
        addEdge(edge);
        connectSymbol(from, to);
    }

    public void disconnect(Vertex v1, String symbolName1, Vertex v2, String symbolName2) {
        Symbol s1 = new Symbol(v1, symbolName1);
        Symbol s2 = new Symbol(v2, symbolName2);
        SEdge edge = new SEdge(s1.getScope(), s2.getScope());
        disconnect(s1, s2);
        if (noConnectionBetweenVertex(v1, v2)) {
            removeEdge(edge);
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


    /**
     * Connect the parent node of symbol from with parent node of to. It will update the edge sets as well.
     *
     * @param from
     * @param to
     */
    private void connectSymbol(Symbol from, Symbol to) {
        IOTableCell fromCell = from.getScope().outputTable.getCellBySymbol(from);
        IOTableCell toCell = to.getScope().inputTable.getCellBySymbol(to);
        if (fromCell != null && toCell != null) {
            fromCell.sendOutputTo(toCell);
        } else {
            Logger.getLogger(this.getClass().getName()).info(String.format("Symbol %s to Symbol %s can not be connected", from, to));
        }
    }

    protected void disconnect(Symbol from, Symbol to) {
        IOTableCell fromCell = from.getScope().outputTable.getCellBySymbol(from);
        IOTableCell toCell = to.getScope().inputTable.getCellBySymbol(to);
        if (fromCell != null && toCell != null) {
            fromCell.removeOutputTo(toCell);
        } else {
            Logger.getLogger(this.getClass().getName()).info(String.format("Symbol %s to Symbol %s can not be connected", from, to));
        }
    }

    public boolean containsNode(Vertex vertex) {
        return nodes.containsKey(vertex.getVertexId());
    }

    private String createLinkLabel(Vertex vertex) {
        List<IOTableCell> outputCells = vertex.getOutputTable().getCells();
        StringJoiner joiner = new StringJoiner(",");
        for (IOTableCell cell : outputCells) {
            joiner.add(cell.getFieldSymbol().getSymbolName());
        }
        return joiner.toString();
    }

    public MutableGraph getVizGraph() {
        MutableGraph g = mutGraph(getVertexId()).setDirected(true).setCluster(true);
        g.graphAttrs().add(Label.of(getVertexId()));
        for (Vertex vertex : this.getNodes()) {
            if (vertex instanceof SNode) {
                SNode v = (SNode) vertex;
                String nodeTitle = v.getVertexId();
                //Simplify the node name
                if (nodeTitle.startsWith("SourceNode")) {
                    nodeTitle = "SourceNode";
                } else if (nodeTitle.startsWith("SinkNode")) {
                    nodeTitle = "SinkNode";
                }
                MutableNode vNode = mutNode(v.getVertexId()).add(Label.of(nodeTitle));
                for (Vertex outputNode : v.getOutputVertices()) { //note: sink node have no outputVertices, parent graph hold this information
                    vNode = createVizEdge(vNode, outputNode);
                }
                g.add(vNode);

            } else {
                SGraph v = (SGraph) vertex;
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
        if (outputNode instanceof SNode) {
            vizFromNode = vizFromNode.addLink(outputNode.getVertexId());
        } else {
            SGraph subGraph = (SGraph) outputNode;
            SNode innerSource = subGraph.sourceNode;
            MutableGraph innerGraph = subGraph.getVizGraph();// create inner graph, this graph is implicitly added to the parent
            vizFromNode = vizFromNode.addLink(innerSource.getVertexId());
        }
        return vizFromNode;
    }

    public void showGraph(String figName) throws IOException {
        Graphviz.fromGraph(getVizGraph()).render(Format.PNG).toFile(new File(String.format("figures/%s.png", figName)));
    }
}
