package ingestionFromStructuredStreaming.fileStream;

import util.SparkContextBuilder;
import ingestionFromStructuredStreaming.utils.Config;
import ingestionFromStructuredStreaming.utils.StreamingUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.System.exit;

/**
 * Created by yifeiliu on 9/28/19.
 * Description:
 */
public class FileStreamConsumerApp {

    private static Logger logger = LoggerFactory.getLogger(FileStreamConsumerApp.class);

    public static void main(String[] args) {
        new FileStreamConsumerApp().start(Config.SchemaMode.WithSchema);
    }

    private void start(Config.SchemaMode schemaMode) {
        logger.info("consumer started: " + schemaMode.name());

        SparkSession spark = SparkContextBuilder.getSparkSession("Read records from file");

        Dataset<Row> df = buildDataframe(spark, schemaMode);

        StreamingQuery query = df.writeStream()
                .outputMode(OutputMode.Update())
                .format("console")
                .option("truncate", false)
                .option("numRows", 3)
                .start();

        try {
            query.awaitTermination(60000);
        } catch (StreamingQueryException e) {
            logger.error(e.getMessage());
        }

        logger.info("consumer exited");
    }

    private Dataset<Row> buildDataframe(SparkSession spark, Config.SchemaMode schemaMode) {
        switch(schemaMode) {
            case WithSchema:
                StructType recordSchema = new StructType()
                        .add("fname", "string")
                        .add("mname", "string")
                        .add("lname", "string")
                        .add("age", "integer")
                        .add("ssn", "string");

                return spark.readStream().format("csv").schema(recordSchema).load(StreamingUtils.getInputDirectory());
            case WithoutSchema:
                return spark.readStream().format("text").load(StreamingUtils.getInputDirectory());
            default:
                logger.error("Unrecognized schema mode: " + schemaMode);
                exit(1);
                break;
        }
        return null;
    }
}
