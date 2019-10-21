package dataTransformation.lowLevelTransformation;

import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by yifeiliu on 10/20/19.
 * Description:
 */
public final class FirstCountyAndStateOfPartition implements MapPartitionsFunction<Row, String> {
    private static final long serialVersionUID = -62694L;

    @Override
    public Iterator<String> call(Iterator<Row> iterator) throws Exception {
        Row row = iterator.next();
        String[] s = row.getAs("Geography").toString().split(", ");
        return Arrays.stream(s).iterator();
    }
}
