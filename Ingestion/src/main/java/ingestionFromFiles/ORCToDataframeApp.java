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
public class ORCToDataframeApp {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        new ORCToDataframeApp().start();
    }

    private void start() {
        Dataset<Row> df = getSparkSession("ORC to dataframe")
                .read().format("orc")
                .load("Ingestion/src/main/resources/demo-11-zlib.orc");

        df.show();
        df.printSchema();
    }
}
