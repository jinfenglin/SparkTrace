package core;

import org.apache.spark.ml.*;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;

/**
 *
 */
public class SparkTraceTaskModel extends PredictionModel<Vector, SparkTraceTaskModel> {
    private static final long serialVersionUID = 5428298810981398146L;


    private PipelineModel ddfModel;
    private PredictionModel predictionModel;

    public SparkTraceTaskModel(PipelineModel ddfModel, PredictionModel predictionModel) {
        this.ddfModel = ddfModel;
        this.predictionModel = predictionModel;
    }

    @Override
    public double predict(Vector vector) {

        return 0;
    }

    @Override
    public String uid() {
        return this.getClass().getName() + serialVersionUID;
    }

    @Override
    public SparkTraceTaskModel copy(ParamMap paramMap) {
        return defaultCopy(paramMap);
    }
}
