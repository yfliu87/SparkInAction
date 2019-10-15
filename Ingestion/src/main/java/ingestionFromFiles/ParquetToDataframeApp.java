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
public class ParquetToDataframeApp {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        new ParquetToDataframeApp().start();
    }

    private void start() {
        Dataset<Row> df = getSparkSession("Parquet to dataframe").read()
                .format("parquet")
                .load("Ingestion/src/main/resources/alltypes_plain.parquet");

        df.show();
        df.printSchema();

    }
}
