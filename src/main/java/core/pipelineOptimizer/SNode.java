package core.pipelineOptimizer;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.param.shared.HasInputCol;
import org.apache.spark.ml.param.shared.HasInputCols;
import org.apache.spark.ml.param.shared.HasOutputCol;
import org.apache.spark.ml.param.shared.HasOutputCols;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;


/**
 * A node in the graph. It contains only IO tables and a pipeline stage that will be integrated
 * into a pipeline through SGraph.
 */
public class SNode extends Vertex {
    private PipelineStage sparkPipelineStage;

    public SNode(PipelineStage pipelineStage) {
        inputTable = new IOTable(this, IOTable.TableType.INPUT_TABLE);
        outputTable = new IOTable(this, IOTable.TableType.OUTPUT_TABLE);
        sparkPipelineStage = pipelineStage;
    }

    public SNode(PipelineStage pipelineStage, String nodeId) {
        inputTable = new IOTable(this, IOTable.TableType.INPUT_TABLE);
        outputTable = new IOTable(this, IOTable.TableType.OUTPUT_TABLE);
        sparkPipelineStage = pipelineStage;
        vertexId = nodeId;
    }

    @Override
    public Pipeline toPipeline() {
        String mismatchedInputColNumMessage = "In node %s pipeline stage expected 1 %s while %s exists";
        Pipeline pipeline = new Pipeline();
        List<IOTableCell> inputCells = getInputTable().getCells();
        List<IOTableCell> outputCells = getOutputTable().getCells();
        //Connect the symbol values with the input and output columns in pipeline
        if (sparkPipelineStage instanceof HasInputCol) {
            if (inputCells.size() != 1) {
                Logger.getLogger(this.getClass().getName()).warning(String.format(mismatchedInputColNumMessage, getVertexId(), "inputColumn", inputCells.size()));
            }
            HasInputCol hasInputColStage = (HasInputCol) sparkPipelineStage;
            hasInputColStage.set(hasInputColStage.inputCol(), inputCells.get(0).getFieldSymbol().getSymbolValue());
        } else if (sparkPipelineStage instanceof HasInputCols) {
            HasInputCols hasInputCols = (HasInputCols) sparkPipelineStage;
            List<String> inputColNames = new ArrayList<>();
            inputCells.forEach(cell -> inputColNames.add(cell.getFieldSymbol().getSymbolValue()));
            hasInputCols.set(hasInputCols.inputCols(), inputColNames.toArray(new String[0]));
        }

        if (sparkPipelineStage instanceof HasOutputCol) {
            if (outputCells.size() != 1) {
                Logger.getLogger(this.getClass().getName()).warning(String.format(mismatchedInputColNumMessage, getVertexId(), "outputColumn", outputCells.size()));
            }
            HasOutputCol hasOutputCol = (HasOutputCol) sparkPipelineStage;
            hasOutputCol.set(hasOutputCol.outputCol(), outputCells.get(0).getFieldSymbol().getSymbolValue());
        } else if (sparkPipelineStage instanceof HasOutputCols) {
            HasOutputCols hasOutputCols = (HasOutputCols) sparkPipelineStage;
            List<String> outputColNames = new ArrayList<>();
            outputCells.stream().forEach(cell -> outputColNames.add(cell.getFieldSymbol().getSymbolValue()));
            hasOutputCols.set(hasOutputCols.outputCols(), outputColNames.toArray(new String[0]));
        }
        pipeline.setStages(new PipelineStage[]{sparkPipelineStage});
        return pipeline;
    }

    public PipelineStage getSparkPipelineStage() {
        return sparkPipelineStage;
    }

    public void setSparkPipelineStage(PipelineStage sparkPipelineStage) {
        this.sparkPipelineStage = sparkPipelineStage;
    }
}
