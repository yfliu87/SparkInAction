import model.Book;
import model.BookMapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

/**
 * Created by yifeiliu on 9/15/19.
 * Description:
 */
public class CsvToDatasetToDataframeApp {

    private SparkSession spark;

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        new CsvToDatasetToDataframeApp().start();
    }

    private void start() {

        spark = SparkSession.builder()
                .appName("dataframe dataset conversion")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load("Basics/src/main/resources/books.csv");


        Dataset<Book> dataset = dataframeToDataset(df);

        datasetToDataframe(dataset);
    }

    private Dataset<Book> dataframeToDataset(Dataset<Row> df) {

        df.printSchema();

        Dataset<Book> bookDataset = df.map(new BookMapper(), Encoders.bean(Book.class));
        System.out.println("**** Book dataset ****");
        bookDataset.show(5, 10);
        bookDataset.printSchema();

        return bookDataset;
    }

    private void datasetToDataframe(Dataset<Book> dataset) {
        System.out.println("**** Book dataframe ****");

        Dataset<Row> df = dataset.toDF();
        df.show(5);

        df = df.withColumn("releaseDateAsString",
                    concat(
                        expr("releaseDate.year + 1900"), lit("-"),
                        expr("releaseDate.month + 1"), lit("-"),
                        df.col("releaseDate.date")))
                .withColumn("releaseDateAsDate",
                        to_date(col("releaseDateAsString"), "yyyy-MM-dd"))
                .drop("releaseDateAsString")
                .drop("releaseDate");

        df.printSchema();
        df.show(5);
    }
}
