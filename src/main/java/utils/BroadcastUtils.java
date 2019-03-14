package utils;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.collection.Seq;
import scala.reflect.ClassTag;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class BroadcastUtils {
    /**
     * key is the commit id and value is the commit time
     *
     * @param dataset
     * @param keyCol
     * @param valueCol
     */
    public static Broadcast<Map<String, Seq<String>>> collectAsBroadcastMapWithSeqValue(Dataset dataset, String keyCol, String valueCol) {
        List<Row> rows = dataset.select(keyCol, valueCol).collectAsList();
        Map<String, Seq<String>> map = new HashMap<>();
        for (Row row : rows) {
            String key = (String) row.get(0);
            Seq<String> value = (Seq<String>) row.get(1);
            map.put(key, value);
        }
        ClassTag<Map<String, Seq<String>>> classTagTest = scala.reflect.ClassTag$.MODULE$.apply(HashMap.class);
        Broadcast<Map<String, Seq<String>>> br = dataset.sparkSession().sparkContext().broadcast(map, classTagTest);
        return br;
    }

    /**
     * Bo
     *
     * @param dataset
     * @param keyCol
     * @param valueCol
     * @return
     */
    public static Broadcast<Map<String, String>> collectAsBroadcastMap(Dataset dataset, String keyCol, String valueCol) {
        List<Row> rows = dataset.select(keyCol, valueCol).collectAsList();
        Map<String, String> map = new HashMap<>();
        for (Row row : rows) {
            String key = (String) row.get(0);
            String value = (String) row.get(1);
            map.put(key, value);
        }
        ClassTag<Map<String, String>> classTagTest = scala.reflect.ClassTag$.MODULE$.apply(HashMap.class);
        Broadcast<Map<String, String>> br = dataset.sparkSession().sparkContext().broadcast(map, classTagTest);
        return br;
    }
}
