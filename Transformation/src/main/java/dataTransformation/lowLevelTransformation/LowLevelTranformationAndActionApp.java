package dataTransformation.lowLevelTransformation;

import org.apache.spark.sql.*;
import util.SparkContextBuilder;

import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.count;

/**
 * Created by yifeiliu on 10/20/19.
 * Description:
 */
public class LowLevelTranformationAndActionApp {
    private static final long serialVersionUID = -17568L;
    private static SparkSession spark;

    public static void main(String[] args) {
        new LowLevelTranformationAndActionApp().start();
    }

    private void start() {
        spark = SparkContextBuilder.getSparkSession("low level transformation and action");

        Dataset<Row> df = loadData();

        transformation(df);
        action(df);
    }

    private Dataset<Row> loadData() {
        System.out.println("Loading data");
        
        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("Transformation/src/main/resources/PEP_2017_PEPANNRES.csv");

        return df.withColumnRenamed("GEO.id", "id")
                .withColumnRenamed("GEO.id2", "id2")
                .withColumnRenamed("GEO.display-label", "Geography")
                .withColumnRenamed("rescen42010", "real2010")
                .drop("resbase42010")
                .withColumnRenamed("respop72010", "estimate2010")
                .withColumnRenamed("respop72011", "estimate2011")
                .withColumnRenamed("respop72012", "estimate2012")
                .withColumnRenamed("respop72013", "estimate2013")
                .withColumnRenamed("respop72014", "estimate2014")
                .withColumnRenamed("respop72015", "estimate2015")
                .withColumnRenamed("respop72016", "estimate2016")
                .withColumnRenamed("respop72017", "estimate2017");
    }

    private void transformation(Dataset<Row> df) {
        System.out.println("Transformation");

        df.map(new CountyFipsExtractor(), Encoders.STRING()).show(5);

        df.filter(new SmallCountiesUsingFilter()).show(5);

        df.flatMap(new CountyStateExtractor(), Encoders.STRING()).show(5);

        Dataset<Row> dfPartitioned = df.repartition(10);
        dfPartitioned.mapPartitions(new FirstCountyAndStateOfPartition(), Encoders.STRING()).show(5);

        df.groupByKey(new StateFipsExtractor(), Encoders.STRING()).count().show(5);

        Dataset<Row> countyStateDf = df
                .withColumn("State", split(df.col("Geography"), ", ").getItem(1))
                .withColumn("County", split(df.col("Geography"), ", ").getItem(0));
        countyStateDf.dropDuplicates("State").show(5);
        countyStateDf.agg(count("County")).show(5);

    }

    private void action(Dataset<Row> df) {
        System.out.println("Action");

        String listOfCountyStateDs = df
                .flatMap(new CountyStateExtractor(), Encoders.STRING())
                .reduce(new CountyStateConcatenator());
        System.out.println(listOfCountyStateDs);

        df.foreach(new DisplayCountyPopulation());
    }
}
