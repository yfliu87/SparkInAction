package dataTransformation.lowLevelTransformation;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

/**
 * Created by yifeiliu on 10/20/19.
 * Description:
 */
public final class CountyFipsExtractor implements MapFunction<Row, String> {
    private static final long serialVersionUID = 26547L;

    @Override
    public String call(Row row) throws Exception {
        return row.getAs("id2").toString().substring(2);
    }
}
