package ingestionFromStructuredStreaming.multipleStreams;

import util.SparkContextBuilder;
import ingestionFromStructuredStreaming.model.RecordReader;
import ingestionFromStructuredStreaming.utils.StreamingUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yifeiliu on 9/28/19.
 * Description:
 */
public class MultipleStreamsConsumerApp {

    private static Logger logger = LoggerFactory.getLogger(MultipleStreamsConsumerApp.class);

    public static void main(String[] args) {
        new MultipleStreamsConsumerApp().start();
    }

    private void start() {
        logger.info("multiple stream consumer started");

        SparkSession spark = SparkContextBuilder.getSparkSession("multiple stream consumer app");

        StructType recordSchema = new StructType()
                .add("fname", "string")
                .add("mname", "string")
                .add("lname", "string")
                .add("age", "integer")
                .add("ssn", "string");

        String dir1 = StreamingUtils.getInputSubDirectory1();
        String dir2 = StreamingUtils.getInputSubDirectory2();

        Dataset<Row> df1 = spark.readStream().format("csv").schema(recordSchema).load(dir1);
        Dataset<Row> df2 = spark.readStream().format("csv").schema(recordSchema).load(dir2);

        StreamingQuery query1 = df1.writeStream().outputMode(OutputMode.Append()).foreach(new RecordReader(1)).start();
        StreamingQuery query2 = df2.writeStream().outputMode(OutputMode.Append()).foreach(new RecordReader(2)).start();

        long startProcessing = System.currentTimeMillis();
        int iterationCount = 0;

        while (query1.isActive() && query2.isActive()) {
            iterationCount++;

            logger.info("Pass #{}", iterationCount);

            if (startProcessing + 60000 < System.currentTimeMillis()) {
                query1.stop();
                query2.stop();
            }

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {

            }
        }
        logger.info("consumer exited");
    }
}
