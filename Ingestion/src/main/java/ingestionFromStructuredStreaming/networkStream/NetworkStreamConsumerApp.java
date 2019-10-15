package ingestionFromStructuredStreaming.networkStream;

import util.SparkContextBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yifeiliu on 9/28/19.
 * Description:
 */
public class NetworkStreamConsumerApp {

    private static Logger logger = LoggerFactory.getLogger(NetworkStreamConsumerApp.class);

    public static void main(String[] args) {
        new NetworkStreamConsumerApp().start();
    }

    private void start() {
        logger.info("consumer started");

        SparkSession spark = SparkContextBuilder.getSparkSession("Read records from network");

        Dataset<Row> df = spark.readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .load();

        StreamingQuery query = df.writeStream()
                .outputMode(OutputMode.Update())
                .format("console")
                .start();

        try {
            query.awaitTermination(60000);
        } catch (StreamingQueryException e) {
            logger.error(e.getMessage());
        }

        logger.info("consumer exited");
    }
}
