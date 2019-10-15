package ingestionFromFiles;

import util.SparkContextBuilder;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Created by yifeiliu on 9/19/19.
 * Description:
 */
public class ComplexCsvToDataframeWithSchemaApp {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        new ComplexCsvToDataframeWithSchemaApp().start();
    }

    private void start() {
        SparkSession spark = SparkContextBuilder.getSparkSession("csv with schema to dataframe");

        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("authorId", DataTypes.IntegerType, true),
                DataTypes.createStructField("bookTitle", DataTypes.StringType, false),
                DataTypes.createStructField("releaseDate", DataTypes.DateType, true),
                DataTypes.createStructField("url", DataTypes.StringType, false)
        });

        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .option("multiline", true)
                .option("sep", ";")
                .option("dateFormat", "M/d/y")
                .option("quote", "*")
                .schema(schema)
                .load("Ingestion/src/main/resources/books.csv");

        df.show(30);
        df.printSchema();
    }
}
