package core.pipelineOptimizer;


import core.graphPipeline.basic.*;
import componentRepo.SLayer.featurePipelineStages.SGraphIOStage;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Get the input source (IOTableCell) for a node.
 */
public class InputSourceSet {
    Set<IOTableCell> inputSources;

    public InputSourceSet(Node node) {
        inputSources = new HashSet<>();
        IOTable inputTable = node.getInputTable();
        for (IOTableCell inputCell : inputTable.getCells()) {
            traceToSource(inputCell);
        }
    }

    private void traceToSource(IOTableCell inputCell) {
        if (inputCell.getInputSource().size() == 0) {
            //If a input cell have no input source then it is the end of search
            inputSources.add(inputCell);
            return;
        }
        IOTableCell inputSourceCell = inputCell.getInputSource().get(0); //One input field should have only 1 source
        Vertex providerVertex = inputSourceCell.getParentTable().getContext();
        if (providerVertex instanceof Node) {
            boolean isNonIOSNode = !(((Node) providerVertex).isIONode());
            if (isNonIOSNode) {
                inputSources.add(inputSourceCell);
            } else {
                //Skip the sourceNode and keep searching
                Graph contextGraph = (Graph) providerVertex.getContext();
                IOTableCell graphInputField = contextGraph.getInputField(inputSourceCell.getFieldSymbol().getSymbolName());
                traceToSource(graphInputField);
            }
        } else {
            //look into the SGraph and keep search
            Graph providerGraph = (Graph) providerVertex;
            IOTableCell sinkNodeReceiverCell = providerGraph.sinkNode.getInputField(inputSourceCell.getFieldSymbol().getSymbolName());
            traceToSource(sinkNodeReceiverCell);
        }
    }

    public void add(IOTableCell inputSource) {
        inputSources.add(inputSource);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InputSourceSet that = (InputSourceSet) o;
        return Objects.equals(inputSources, that.inputSources);
    }

    @Override
    public int hashCode() {
        return Objects.hash(inputSources);
    }
}
