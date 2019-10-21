package dataTransformation.lowLevelTransformation;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Row;

/**
 * Created by yifeiliu on 10/20/19.
 * Description:
 */
public final class DisplayCountyPopulation implements ForeachFunction<Row> {
    private static final long serialVersionUID = 14738L;
    private int count = 0;

    @Override
    public void call(Row row) throws Exception {
        if (count < 10) {
            System.out.println(row.getAs("Geography").toString() + " had " + row.getAs("real2010").toString() + " inhabitants in 2010.");
        }
        count++;
    }
}
