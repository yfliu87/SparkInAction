import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.col;

/**
 * Created by yifeiliu on 9/15/19.
 * Description:
 */
public class CombineDataFrames {

    private SparkSession spark;

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        CombineDataFrames combineDF = new CombineDataFrames();
        combineDF.start();
    }

    private void start() {
        this.spark = SparkSession.builder()
                .appName("union of dataframes")
                .master("local")
                .getOrCreate();

        Dataset<Row> wakeRestaurantDF = this.buildWakeRestaurantsDataframe();
        Dataset<Row> durhamRestaurantDF = this.buildDurhamRestaurantsDataframe();
        combineDataframes(wakeRestaurantDF, durhamRestaurantDF);
    }

    private Dataset<Row> buildWakeRestaurantsDataframe() {
        Dataset<Row> df = this.spark.read().format("csv")
                .option("header", "true")
                .load("Basics/src/main/resources/restaurants_in_wake_county_nc.csv");

        return df.withColumn("county", lit("Wake"))
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
                .withColumn("dateEnd", lit(null))
                .withColumn("id", concat(col("state"), lit("_"), col("county"), lit("_"), col("datasetId")))
                .drop("OBJECTID")
                .drop("PERMITID")
                .drop("GEOCODESTATUS");
    }

    private Dataset<Row> buildDurhamRestaurantsDataframe() {
        Dataset<Row> df = this.spark.read().format("json")
                .option("header", "true")
                .load("Basics/src/main/resources/restaurants_in_Durham_county_nc.json");

        return df.withColumn("county", lit("Durham"))
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
                .withColumn("geoY", df.col("fields.geolocation").getItem(1))
                .withColumn("id", concat(col("state"), lit("_"), col("county"), lit("_"), col("datasetId")))
                .drop(df.col("fields"))
                .drop(df.col("geometry"))
                .drop(df.col("record_timestamp"))
                .drop(df.col("recordid"));
    }

    private void combineDataframes(Dataset<Row> wakeRestaurantDF, Dataset<Row> durhamRestaurantDF) {
        Dataset<Row> df = wakeRestaurantDF.unionByName(durhamRestaurantDF);
        df.printSchema();
        System.out.println("Union records # : " + df.count());
        System.out.println("Union records partition # : " + df.rdd().partitions().length);

        df = df.withColumnRenamed("id","uniqueId");
        df.explain();
    }

}
