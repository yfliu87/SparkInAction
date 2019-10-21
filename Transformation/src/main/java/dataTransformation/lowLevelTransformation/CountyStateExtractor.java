package dataTransformation.lowLevelTransformation;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by yifeiliu on 10/20/19.
 * Description:
 */
public final class CountyStateExtractor implements FlatMapFunction<Row, String> {
    private static final long serialVersionUID = 63784L;

    @Override
    public Iterator<String> call(Row row) throws Exception {
        String[] s = row.getAs("Geography").toString().split(", ");
        return Arrays.stream(s).iterator();
    }
}
