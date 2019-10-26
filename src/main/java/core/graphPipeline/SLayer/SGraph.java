package core.graphPipeline.SLayer;

import core.graphPipeline.SDF.SDFInterface;
import core.graphPipeline.basic.Edge;
import core.graphPipeline.basic.Graph;
import core.graphPipeline.basic.IOTableCell;
import core.graphPipeline.basic.Vertex;
import core.graphPipeline.graphSymbol.Symbol;
import core.graphPipeline.graphSymbol.SymbolTable;
import featurePipelineStages.SGraphColumnRemovalStage;
import featurePipelineStages.SGraphIOStage;
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
public class SGraph extends Graph implements SDFInterface, SLayerComponent {


    public enum SDFType {
        SOURCE_SDF,
        TARGET_SDF,
    }

    private Map<String, SDFType> outputTypeMap;

    public SGraph() {
        super();
        nodes = new HashMap<>();
        edges = new HashSet<>();
        sourceNode = new SNode(new SGraphIOStage());
        sinkNode = new SNode(new SGraphIOStage());
        sourceNode.setVertexLabel("SourceNode");
        sinkNode.setVertexLabel("SinkNode");
        sourceNode.setContext(this);
        sinkNode.setContext(this);
        nodes.put(sourceNode.getVertexId(), sourceNode);
        nodes.put(sinkNode.getVertexId(), sinkNode);
        config = new HashMap<>();
        outputTypeMap = new HashMap<>();
    }

    public SGraph(String graphId) {
        super(graphId);
        nodes = new HashMap<>();
        edges = new HashSet<>();
        sourceNode = new SNode(new SGraphIOStage());
        sinkNode = new SNode(new SGraphIOStage());
        sourceNode.setVertexLabel("SourceNode");
        sinkNode.setVertexLabel("SinkNode");
        sourceNode.setContext(this);
        sinkNode.setContext(this);
        nodes.put(sourceNode.getVertexId(), sourceNode);
        nodes.put(sinkNode.getVertexId(), sinkNode);
        config = new HashMap<>();
        outputTypeMap = new HashMap<>();
    }

    @Override
    public Vertex addOutputField(Symbol symbol) throws Exception {
        super.addOutputField(symbol);
        outputTypeMap.put(symbol.getSymbolName(), null);
        return this;
    }

    @Override
    public void removeOutputField(Symbol symbol) {
        super.removeOutputField(symbol);
        outputTypeMap.remove(symbol.getSymbolName());
    }

    /**
     * Make sure the edges which contains deleted nodes are also deleted.
     */
    private void clearEdge() {
        for (Edge edge : new ArrayList<>(edges)) {
            if (!nodes.values().contains(edge.getFrom()) || !nodes.values().contains(edge.getTo())) {
                edges.remove(edge);
            }
        }
    }

    @Override
    public Pipeline toPipeline() throws Exception {
        clearEdge();
        syncSymbolValues(this);

        //Config the SGraphIOStage to parse the InputTable which translate the Symbols to real column names
        SGraphIOStage initStage = (SGraphIOStage) ((SNode) sourceNode).getSparkPipelineStage();
        initStage.setInputCols(inputTable);
        initStage.setOutputCols(inputTable);//IOTable consumed by inner method mapIOTableToIOParam(), the graph's inputTabel will be used

        //Config the SGraphSinkStage
        SGraphIOStage outStage = (SGraphIOStage) ((SNode) sinkNode).getSparkPipelineStage();
        outStage.setInputCols(outputTable);
        outStage.setOutputCols(outputTable);

        //Add Stages and create column cleaning stages in fly
        List<Vertex> topSortNodes = topologicalSort(this);
        Pipeline pipeline = new Pipeline(getVertexId());
        List<PipelineStage> stages = new ArrayList<>();
        Map<String, Integer> demandTable = getDemandTable();
        for (Vertex node : topSortNodes) {
            if (node == sinkNode || node == sourceNode)
                continue;
            stages.addAll(Arrays.asList(((SLayerComponent) node).toPipeline().getStages()));
            //Add column clean stages
            if (!node.equals(sinkNode)) {
                for (IOTableCell targetCell : node.getInputTable().getCells()) {
                    for (IOTableCell sourceCell : targetCell.getInputSource()) {
                        if (sourceCell.getParentTable().getContext().equals(sourceNode) && this.getContext() != null) {
                            continue; //Source node contains the fields that belong to parent graph
                        }
                        String sourceCellSymbolValue = sourceCell.getFieldSymbol().getSymbolValue();
                        if (demandTable.get(sourceCellSymbolValue) == null) {
                            continue;
                        }
                        int remainDemand = demandTable.get(sourceCellSymbolValue) - 1;
                        demandTable.put(sourceCellSymbolValue, remainDemand);
                        if (remainDemand == 0 && sourceCell.isRemovable()) {
                            SGraphColumnRemovalStage removalStage = new SGraphColumnRemovalStage();
                            removalStage.setInputCols(new String[]{sourceCellSymbolValue});
                            stages.add(removalStage);
                        }
                    }
                }
            }
        }
        pipeline.setStages(stages.toArray(new PipelineStage[0]));
        return pipeline;
    }


    @Override
    public Map<String, SDFType> getOutputTypeMap() {
        return outputTypeMap;
    }

    private Set<String> getSymbolValuesByName(Set<String> names) {
        Set<String> symbolValues = new HashSet<>();
        for (String name : names) {
            String value = SymbolTable.getSymbolValue(new Symbol(this, name));
            symbolValues.add(value);
        }
        return symbolValues;
    }


    @Override
    public Set<String> getTargetSDFOutputs() {
        Set<String> symbolNames = splitSDFOutputs().getValue();
        return getSymbolValuesByName(symbolNames);
    }

    @Override
    public Set<String> getSourceSDFOutput() {
        Set<String> symbolNames = splitSDFOutputs().getKey();
        return getSymbolValuesByName(symbolNames);
    }

    public Vertex addOutputField(Symbol symbol, SDFType type) throws Exception {
        this.addOutputField(symbol);
        assignTypeToOutputField(symbol.getSymbolName(), type);
        return this;
    }

    public Vertex addOutputField(String symbolName, SDFType type) throws Exception {
        Symbol symbol = new Symbol(this, symbolName);
        this.addOutputField(symbol, type);
        return this;
    }
}
