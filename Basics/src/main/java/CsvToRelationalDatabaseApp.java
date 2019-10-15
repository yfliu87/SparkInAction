import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

import static org.apache.spark.sql.functions.*;

/**
 * Created by yifeiliu on 9/14/19.
 * Description:
 */
public class CsvToRelationalDatabaseApp {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        CsvToRelationalDatabaseApp app = new CsvToRelationalDatabaseApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("csv to db")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "true")
                .load("Basics/src/main/resources/authors.csv");

        df = df.withColumn("name", concat(df.col("lname"), lit(", "), df.col("fname")));

        String dbConnectionURL = "jdbc:postgresql://localhost/SparkInAction";

        Properties prop = new Properties();
        prop.setProperty("driver", "org.postgresql.Driver");
        prop.setProperty("user", "yifei");
        prop.setProperty("password", "yfliu1987");

        df.write().mode(SaveMode.Overwrite).jdbc(dbConnectionURL, "ch02", prop);

        System.out.println("Process complete");
    }
}
