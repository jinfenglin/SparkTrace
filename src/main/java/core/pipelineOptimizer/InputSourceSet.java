package core.pipelineOptimizer;

import core.graphPipeline.basic.*;
import featurePipeline.SGraphIOStage;
import org.apache.parquet.io.InputFile;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Get the input source (IOTableCell) for a node.
 */
public class InputSourceSet {
    Set<IOTableCell> inputSources;

    public InputSourceSet(SNode node) {
        inputSources = new HashSet<>();
        IOTable inputTable = node.getInputTable();
        for (IOTableCell inputCell : inputTable.getCells()) {
            traceToSource(inputCell);
        }
    }

    private void traceToSource(IOTableCell inputCell) {
        IOTableCell inputSourceCell = inputCell.getInputSource().get(0); //One input field should have only 1 source
        Vertex providerVertex = inputSourceCell.getParentTable().getContext();
        if (providerVertex instanceof SNode) {
            boolean isNonIOSNode = !(((SNode) providerVertex).getSparkPipelineStage() instanceof SGraphIOStage);
            if (isNonIOSNode) {
                inputSources.add(inputSourceCell);
            } else {
                SGraph contextGraph = (SGraph) providerVertex.getContext();
                IOTableCell graphInputField = contextGraph.getInputField(inputCell.getFieldSymbol().getSymbolName());
                if (graphInputField == null) {
                    inputSources.add(inputSourceCell);
                } else {
                    traceToSource(graphInputField);
                }
            }
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
