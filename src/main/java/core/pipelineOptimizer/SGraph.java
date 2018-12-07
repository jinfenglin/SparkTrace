package core.pipelineOptimizer;

import core.GraphSymbol.Symbol;
import core.GraphSymbol.SymbolTable;
import featurePipeline.SGraphIOStage;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;

import java.util.*;
import java.util.logging.Logger;

/**
 * A DAG which will be applied to optimize the pipeline structure.
 * To create a graph:
 * 1. Define the IO table for the graph by setting the inputTable and outputTable
 * 2. Add node to the graphs
 * 3. connect the graphs, and remember to connect to the sourceNode and sinkNode
 * 4. Add penetrations to the graph
 * 5. Call toPipeline to create a fully configured and runnable pipeline
 */
public class SGraph extends Vertex {
    private Map<String, Vertex> nodes;
    private Set<SEdge> edges; //Record the node level connection, the field level connection is recorded by the IOTable

    public SNode sourceNode, sinkNode;

    public SGraph() {
        nodes = new HashMap<>();
        edges = new HashSet<>();
        sourceNode = new SNode(new SGraphIOStage());
        sinkNode = new SNode(new SGraphIOStage());
        nodes.put(sourceNode.vertexId, sourceNode);
        nodes.put(sinkNode.vertexId, sinkNode);
    }

    public SGraph(String graphId) {
        nodes = new HashMap<>();
        edges = new HashSet<>();
        sourceNode = new SNode(new SGraphIOStage());
        sinkNode = new SNode(new SGraphIOStage());
        nodes.put(sourceNode.vertexId, sourceNode);
        nodes.put(sinkNode.vertexId, sinkNode);
        vertexId = graphId;
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
        //sourceNode.addInputField(addedSymbol);
        sourceNode.addOutputField(addedSymbol);
        return this;
    }

    @Override
    public Vertex addOutputField(Symbol symbol) throws Exception {
        super.addOutputField(symbol);
        Symbol addedSymbol = new Symbol(sinkNode, symbol.getSymbolName());
        sinkNode.addInputField(addedSymbol);
        //sinkNode.addOutputField(addedSymbol);
        return this;
    }

    /**
     * Let the connected symbols share same value
     */
    private void syncSymbolValues(SGraph graph) throws Exception {
        //SourceNode and SinkNode are special in the graph. The sourceNode don't have connection on its InputTable,
        //Instead, it consume graph's InputTable directly. However, the consumption is through parameters thus no explicit
        //connections are specified between graph.inputTable and sourceNode.outputTable (And inputTable should receive and have no out going links)
        for (IOTableCell graphInputCell : this.getInputTable().getCells()) {
            Symbol graphProviderSymbol = graphInputCell.getFieldSymbol();
            Symbol sourceNodeReceiverSymbol = sourceNode.getOutputTable().getSymbolByVarName(graphProviderSymbol.getSymbolName());
            SymbolTable.shareSymbolValue(graphProviderSymbol, sourceNodeReceiverSymbol, false);
        }

        for (IOTableCell graphOutputCell : this.getOutputTable().getCells()) {
            Symbol graphReceiverSymbol = graphOutputCell.getFieldSymbol();
            Symbol sinkNodeProviderSymbol = sinkNode.getInputTable().getSymbolByVarName(graphReceiverSymbol.getSymbolName());
            SymbolTable.shareSymbolValue(sinkNodeProviderSymbol, graphReceiverSymbol, false);
        }

        for (Vertex node : graph.getNodes()) {
            for (IOTableCell providerCell : node.outputTable.getCells()) {
                Symbol providerSymbol = providerCell.getFieldSymbol();
                for (IOTableCell receiverCell : providerCell.getOutputTarget()) {
                    Symbol receiverSymbol = receiverCell.getFieldSymbol();
                    SymbolTable.shareSymbolValue(providerSymbol, receiverSymbol, true);
                }
            }
            if (node instanceof SGraph) {
                syncSymbolValues((SGraph) node);
            }
        }
    }

    @Override
    public Pipeline toPipeline() throws Exception {
        //Config the SGraphIOStage to parse the InputTable which translate the Symbols to real column names
        SGraphIOStage initStage = (SGraphIOStage) sourceNode.getSparkPipelineStage();
        initStage.setInputCols(inputTable);
        initStage.setOutputCols(inputTable);//IOTable consumed by inner method mapIOTableToIOParam(), the graph's inputTabel will be used

        //Config the SGraphSinkStage
        SGraphIOStage outStage = (SGraphIOStage) sinkNode.getSparkPipelineStage();
        outStage.setInputCols(outputTable);
        outStage.setOutputCols(outputTable);

        syncSymbolValues(this);
        Pipeline pipeline = new Pipeline();
        List<PipelineStage> stages = new ArrayList<>();
        List<Vertex> topSortNodes = topologicalSort(this);
        for (Vertex node : topSortNodes) {
            stages.add(node.toPipeline());
        }
        pipeline.setStages(stages.toArray(new PipelineStage[0]));
        return pipeline;
    }

    private static Map<Vertex, List<Vertex>> buildIndex(SGraph graph, boolean useFromAsIndex) {
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
            inDegreeMap.put(to, inDegreeMap.get(to) + 1);
        }
        return inDegreeMap;
    }

    private static List<Vertex> topologicalSort(SGraph graph) throws Exception {
        List<Vertex> nodes = new ArrayList<>(graph.getNodes());
        Map<Vertex, List<Vertex>> edgeIndex = buildIndex(graph, true);
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


    public List<Vertex> getNodes() {
        return new ArrayList<>(nodes.values());
    }

    public List<SEdge> getEdges() {
        return new ArrayList<>(edges);
    }

    public void addNode(Vertex node) {
        nodes.put(node.getVertexId(), node);
    }

    public void addEdge(SEdge edge) {
        edges.add(edge);
    }

    public void connect(Vertex v1, String symbolName1, Vertex v2, String symbolName2) {
        Symbol s1 = new Symbol(v1, symbolName1);
        Symbol s2 = new Symbol(v2, symbolName2);
        SEdge edge = new SEdge(s1.getScope(), s2.getScope());
        addEdge(edge);
        connect(s1, s2);
    }

    /**
     * Connect the parent node of symbol from with parent node of to. It will update the edge sets as well.
     *
     * @param from
     * @param to
     */
    private void connect(Symbol from, Symbol to) {
        IOTableCell fromCell = from.getScope().outputTable.getCellBySymbol(from);
        IOTableCell toCell = to.getScope().inputTable.getCellBySymbol(to);
        if (fromCell != null && toCell != null) {
            fromCell.sendOutputTo(toCell);
        } else {
            Logger.getLogger(this.getClass().getName()).info(String.format("Symbol %s to Symbol %s can not be connected", from, to));
        }
    }
}
