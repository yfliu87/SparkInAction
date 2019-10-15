package ingestionFromFiles;

import util.SparkContextBuilder;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Created by yifeiliu on 9/19/19.
 * Description:
 */
public class MultilineJsonToDataframeApp {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        new MultilineJsonToDataframeApp().start();
    }

    private void start() {
        Dataset<Row> df = SparkContextBuilder.getSparkSession("Multiline JSON to dataframe")
                .read().format("json")
                .option("multiline", true)
                .load("Ingestion/src/main/resources/countrytravelinfo.json");

        df.show(10);
        df.printSchema();
    }
}
