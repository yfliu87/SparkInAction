package ingestionFromFiles;

import util.SparkContextBuilder;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Created by yifeiliu on 9/21/19.
 * Description:
 */
public class XMLToDataframeApp {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        new XMLToDataframeApp().start();
    }

    private void start() {
        Dataset<Row> df = SparkContextBuilder.getSparkSession("XML to dataframe")
                .read().format("xml")
                .option("rowTag", "row")
                .load("Ingestion/src/main/resources/nasa-patents.xml");

        df.show(10);
        df.printSchema();
    }
}
