package dataTransformation.lowLevelTransformation;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;

/**
 * Created by yifeiliu on 10/20/19.
 * Description:
 */
public final class SmallCountiesUsingFilter implements FilterFunction<Row> {
    private static final long serialVersionUID = 17392L;

    @Override
    public boolean call(Row row) throws Exception {
        return row.getInt(4) < 30000;
    }
}
