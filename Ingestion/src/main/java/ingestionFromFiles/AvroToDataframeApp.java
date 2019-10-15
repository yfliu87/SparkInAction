package ingestionFromFiles;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static util.SparkContextBuilder.getSparkSession;

/**
 * Created by yifeiliu on 9/21/19.
 * Description:
 */
public class AvroToDataframeApp {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        new AvroToDataframeApp().start();
    }

    private void start() {
        Dataset<Row> df = getSparkSession("Avro to dataframe").read()
                .format("com.databricks.spark.avro")
                .load("Ingestion/src/main/resources/weather.avro");

        df.show();
        df.printSchema();
    }
}
