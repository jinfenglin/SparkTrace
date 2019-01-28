package core.graphPipeline.basic;

import featurePipeline.NullRemoveWrapper.HasInnerStage;
import featurePipeline.NullRemoveWrapper.InnerStageImplementHasInputCol;
import featurePipeline.NullRemoveWrapper.InnerStageImplementHasOutputCol;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.param.shared.HasInputCol;
import org.apache.spark.ml.param.shared.HasInputCols;
import org.apache.spark.ml.param.shared.HasOutputCol;
import org.apache.spark.ml.param.shared.HasOutputCols;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.logging.Logger;

import static core.pipelineOptimizer.PipelineOptimizer.*;


/**
 * A node in the graph. It contains only IO tables and a pipeline stage that will be integrated
 * into a pipeline through SGraph.
 */
public class SNode extends Vertex {
    private PipelineStage sparkPipelineStage;
    private static String stageMoIOInterfaceErrorMsg = "The inner Spark %s is not implementing" +
            "%s interface, thus the input column value can be not " +
            "be passed to this stage";

    public SNode(PipelineStage pipelineStage) {
        super();
        sparkPipelineStage = pipelineStage;
    }

    public SNode(PipelineStage pipelineStage, String nodeId) {
        super(nodeId);
        sparkPipelineStage = pipelineStage;
    }

    @Override
    public Pipeline toPipeline() throws Exception {
        String mismatchedInputColNumMessage = "In node %s pipeline stage expected 1 %s while %s exists";
        Pipeline pipeline = new Pipeline(getVertexId());
        List<IOTableCell> inputCells = getInputTable().getCells();
        List<IOTableCell> outputCells = getOutputTable().getCells();
        //Connect the symbol values with the input and output columns in pipeline
        if (sparkPipelineStage instanceof HasInputCol) {
            if (inputCells.size() != 1) {
                Logger.getLogger(this.getClass().getName()).warning(String.format(mismatchedInputColNumMessage, getVertexId(), "inputColumn", inputCells.size()));
            }
            HasInputCol hasInputColStage = (HasInputCol) sparkPipelineStage;
            String inputColName = inputCells.get(0).getFieldSymbol().getSymbolValue();
            if (hasInputColStage instanceof InnerStageImplementHasInputCol) {
                ((InnerStageImplementHasInputCol) hasInputColStage).setInputCol(inputColName);
            } else {
                hasInputColStage.set(hasInputColStage.inputCol(), inputColName);
            }
        } else if (sparkPipelineStage instanceof HasInputCols) {
            HasInputCols hasInputCols = (HasInputCols) sparkPipelineStage;
            List<String> inputColNames = new ArrayList<>();
            inputCells.forEach(cell -> inputColNames.add(cell.getFieldSymbol().getSymbolValue()));
            //TODO add if (hasInputCols instanceof InnerStageImplementHasInputCols)
            hasInputCols.set(hasInputCols.inputCols(), inputColNames.toArray(new String[0]));
        } else {
            throw new Exception(stageMoIOInterfaceErrorMsg.format(sparkPipelineStage.toString(), "HasInputCol/HasInputCols"));
        }

        if (sparkPipelineStage instanceof HasOutputCol) {
            if (outputCells.size() != 1) {
                Logger.getLogger(this.getClass().getName()).warning(String.format(mismatchedInputColNumMessage, getVertexId(), "outputColumn", outputCells.size()));
            }
            HasOutputCol hasOutputCol = (HasOutputCol) sparkPipelineStage;
            String outputColName = outputCells.get(0).getFieldSymbol().getSymbolValue();
            if (hasOutputCol instanceof InnerStageImplementHasOutputCol) {
                ((InnerStageImplementHasOutputCol) hasOutputCol).setOutputCol(outputColName);
            } else {
                hasOutputCol.set(hasOutputCol.outputCol(), outputColName);
            }

        } else if (sparkPipelineStage instanceof HasOutputCols) {
            HasOutputCols hasOutputCols = (HasOutputCols) sparkPipelineStage;
            List<String> outputColNames = new ArrayList<>();
            outputCells.stream().forEach(cell -> outputColNames.add(cell.getFieldSymbol().getSymbolValue()));
            //TODO add if (hasInputCols instanceof InnerStageImplementHasOutputCols)
            hasOutputCols.set(hasOutputCols.outputCols(), outputColNames.toArray(new String[0]));
        } else {
            throw new Exception(stageMoIOInterfaceErrorMsg.format(sparkPipelineStage.toString(), "HasOutputCol/HasOutputCols"));
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

    @Override
    public String toString() {
        String snodeStr = String.format("%s|%s", getStageIds(getSparkPipelineStage()), getNonIOParamsValue(getSparkPipelineStage()));
        return snodeStr;
    }

    private List<String> getNonIOParamsValue(PipelineStage stage) {
        List<Param> params = new ArrayList<>(Arrays.asList(stage.params()));
        List<String> paramValues = new ArrayList<>();
        if (stage instanceof HasInputCols) {
            params.remove(((HasInputCols) stage).inputCols());
        } else if (stage instanceof HasInputCol) {
            params.remove(((HasInputCol) stage).inputCol());
        }
        if (stage instanceof HasOutputCol) {
            params.remove(((HasOutputCol) stage).outputCol());
        } else if (stage instanceof HasOutputCols) {
            params.remove(((HasOutputCols) stage).outputCols());
        }
        for (Param param : params) {
            paramValues.add(stage.get(param).toString());
        }
        if (stage instanceof HasInnerStage) {
            List<String> innerParamValues = getNonIOParamsValue(((HasInnerStage) stage).getInnerStage());
            paramValues.addAll(innerParamValues);
        }
        return paramValues;
    }

    private List<String> getStageIds(PipelineStage stage) {
        List<String> stageIds = new ArrayList<>();
        stageIds.add(stage.uid());
        if (stage instanceof HasInnerStage) {
            List<String> innerIds = getStageIds(stage);
            stageIds.addAll(innerIds);
        }
        return stageIds;
    }
}
