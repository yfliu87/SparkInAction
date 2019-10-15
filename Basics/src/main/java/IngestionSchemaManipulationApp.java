import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

/**
 * Created by yifeiliu on 9/14/19.
 * Description:
 */
public class IngestionSchemaManipulationApp {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        IngestionSchemaManipulationApp app = new IngestionSchemaManipulationApp();
        app.start();
    }

    private void start() {
        loadCSV();
        loadJSON();
    }

    private void loadJSON() {
        SparkSession spark = SparkSession.builder()
                .appName("Restaurants in wake county, NC")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("json").load("Basics/src/main/resources/restaurants_in_Durham_county_nc.json");

        System.out.println("\n\n*** Load JSON");
        df.show(5);
        df.printSchema();

        df = df.withColumn("county", lit("Durham"))
                .withColumn("datasetId", df.col("fields.id"))
                .withColumn("name", df.col("fields.premise_name"))
                .withColumn("address1", df.col("fields.premise_address1"))
                .withColumn("address2", df.col("fields.premise_address2"))
                .withColumn("city", df.col("fields.premise_city"))
                .withColumn("state", df.col("fields.premise_state"))
                .withColumn("zip", df.col("fields.premise_zip"))
                .withColumn("tel", df.col("fields.premise_phone"))
                .withColumn("dateStart", df.col("fields.opening_date"))
                .withColumn("dateEnd", df.col("fields.closing_date"))
                .withColumn("type", split(df.col("fields.type_description"), " - ").getItem(1))
                .withColumn("geoX", df.col("fields.geolocation").getItem(0))
                .withColumn("geoY", df.col("fields.geolocation").getItem(1));

        df = df.withColumn("id", concat(df.col("state"), lit("_"), df.col("county"), lit("_"), df.col("datasetId")));

        System.out.println("\n data frame transformation");
        df.show(5);
        df.printSchema();

        System.out.println("Partition #: " + df.rdd().partitions().length);
    }

    private void loadCSV() {

        SparkSession spark = SparkSession.builder()
                .appName("Restaurants in wake county, NC")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("csv").option("header","true").load("Basics/src/main/resources/restaurants_in_wake_county_nc.csv");
        System.out.printf("*** Load CSV");
        System.out.printf("%d records loaded", df.count());
        df.printSchema();
        df.show(5);

        df = df.withColumn("county", lit("Wake"))
                .withColumnRenamed("HSISID", "datasetId")
                .withColumnRenamed("NAME", "name")
                .withColumnRenamed("ADDRESS1", "address1")
                .withColumnRenamed("ADDRESS2", "address2")
                .withColumnRenamed("CITY", "city")
                .withColumnRenamed("STATE", "state")
                .withColumnRenamed("POSTALCODE", "zip")
                .withColumnRenamed("PHONENUMBER", "tel")
                .withColumnRenamed("RESTAURANTOPENDATE", "dateStart")
                .withColumnRenamed("FACILITYTYPE", "type")
                .withColumnRenamed("X", "geoX")
                .withColumnRenamed("Y", "geoY")
                .drop("OBJECTID")
                .drop("PERMITID")
                .drop("GEOCODESTATUS");

        df = df.withColumn("id", concat(df.col("state"), lit("_"), df.col("county"), lit("_"), df.col("datasetId")));

        System.out.printf("\n\nAfter transformation");
        df.printSchema();
        df.show(5);

        System.out.println("**** Partitions: " + df.rdd().partitions().length);

        System.out.println("**** Schema as json: " + df.schema().prettyJson());
    }
}
