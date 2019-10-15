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
public class TextToDataframeApp {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        new TextToDataframeApp().start();
    }

    private void start() {
        Dataset<Row> df = SparkContextBuilder.getSparkSession("Text to dataframe")
                .read()
                .format("text")
                .load("Ingestion/src/main/resources/romeo-juliet.txt");

        df.show(10);
        df.printSchema();
    }
}
