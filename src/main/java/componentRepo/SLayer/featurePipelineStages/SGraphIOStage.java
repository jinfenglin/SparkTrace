package componentRepo.SLayer.featurePipelineStages;

import core.graphPipeline.graphSymbol.Symbol;
import core.graphPipeline.basic.IOTable;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.param.StringArrayParam;
import org.apache.spark.ml.param.shared.HasInputCols;
import org.apache.spark.ml.param.shared.HasOutputCols;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.util.*;


/**
 * Role of this stage:
 * 1. Provide a consistent interface to the SGraph
 * 2. Ensure the correctness of the input data frame
 * 3. Translate and assign the symbol to real column names
 * <p>
 * The setInputCols() and setOutputCols() must be called.
 */
public class SGraphIOStage extends Transformer implements HasInputCols, HasOutputCols {
    private static final long serialVersionUID = 4692636407800327072L;
    StringArrayParam inputCols;
    StringArrayParam outputCols;

    public SGraphIOStage() {
        inputCols = initInputCols();
        outputCols = initOutputCols();
    }

    private void mapIOTableToIOParam(IOTable table, StringArrayParam IOParam) {
        List<Symbol> inputSymbol = table.getSymbols();
        List<String> inputColNames = new ArrayList<>();
        for (Symbol inSym : inputSymbol) {
            inputColNames.add(inSym.getSymbolValue());
        }
        set(IOParam, inputColNames.toArray(new String[0]));
    }

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        return (Dataset<Row>) dataset;
    }

    @Override
    public StructType transformSchema(StructType structType) {
        return structType;
    }

    @Override
    public Transformer copy(ParamMap paramMap) {
        return defaultCopy(paramMap);
    }

    @Override
    public String uid() {
        return this.getClass().getName() + serialVersionUID;
    }


    @Override
    public void org$apache$spark$ml$param$shared$HasInputCols$_setter_$inputCols_$eq(StringArrayParam stringArrayParam) {

    }

    @Override
    public StringArrayParam inputCols() {
        return inputCols;
    }

    public StringArrayParam initInputCols() {
        return new StringArrayParam(this, "inputCols", "input columns for a vertex in SGraph");
    }

    @Override
    public String[] getInputCols() {
        return getOrDefault(inputCols);
    }

    public void setInputCols(IOTable inputTable) {
        mapIOTableToIOParam(inputTable, inputCols);
    }

    @Override
    public void org$apache$spark$ml$param$shared$HasOutputCols$_setter_$outputCols_$eq(StringArrayParam stringArrayParam) {

    }

    @Override
    public StringArrayParam outputCols() {
        return outputCols;
    }

    public StringArrayParam initOutputCols() {
        return new StringArrayParam(this, "outputCols", "output columns for a vertex in SGraph");
    }

    @Override
    public String[] getOutputCols() {
        return getOrDefault(outputCols);
    }

    public void setOutputCols(IOTable outputTable) {
        mapIOTableToIOParam(outputTable, outputCols);
    }
}
