package utils;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 *
 */
public class SchemaUtil {
    public static StructType createArtifactSchema(String id, String content) {
        StructType schema = new StructType(new StructField[]{
                new StructField(id, DataTypes.StringType, true, Metadata.empty()),
                new StructField(content, DataTypes.StringType, true, Metadata.empty())
        });
        return schema;
    }
}
