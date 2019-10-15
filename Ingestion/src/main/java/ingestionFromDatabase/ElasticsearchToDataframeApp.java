package ingestionFromDatabase;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static util.SparkContextBuilder.getSparkSession;

/**
 * Created by yifeiliu on 9/22/19.
 * Description:
 */
public class ElasticsearchToDataframeApp {

    private static SparkSession spark;

    public static void main(String[] args) {
        spark = getSparkSession("Elasticsearch to dataframe");

        new ElasticsearchToDataframeApp().start();
    }

    private void start() {
        Dataset<Row> df = spark.read().format("org.elasticsearch.spark.sql")
                .option("es.nodes", "localhost")
                .option("es.port", "9200")
                .option("es.query", "?q=*")
                .option("es.read.field.as.array.include", "Inspection_Date")
                .load("nyc_restaurants");

        df.show();
        df.printSchema();
        System.out.println("The dataframe is split over " + df.rdd() .getPartitions().length + " partition(s).");
    }
}
