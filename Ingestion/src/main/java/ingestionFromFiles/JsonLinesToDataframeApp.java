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
public class JsonLinesToDataframeApp {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        new JsonLinesToDataframeApp().start();
    }

    private void start() {
        Dataset<Row> df = SparkContextBuilder.getSparkSession("JSON lines to dataframe")
                .read().format("json")
                .load("Ingestion/src/main/resources/durham-nc-foreclosure-2006-2016.json");

        df.show(10);
        df.printSchema();
    }
}
