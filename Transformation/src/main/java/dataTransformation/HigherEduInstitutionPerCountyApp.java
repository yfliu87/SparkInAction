package dataTransformation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import util.SparkContextBuilder;

import static org.apache.spark.sql.functions.*;

/**
 * Created by yifeiliu on 10/20/19.
 * Description:
 */
public class HigherEduInstitutionPerCountyApp {

    private static SparkSession spark;

    public static void main(String[] args) {
        new HigherEduInstitutionPerCountyApp().start();
    }

    private void start() {
        spark = SparkContextBuilder.getSparkSession("Join");

        Dataset<Row> censusDf = loadCensusData();
        Dataset<Row> higherEduDf = loadEduData();
        Dataset<Row> countyZipDf = loadCountyZipData();

        Dataset<Row> institutionPerCountyDf = joinDatasets(censusDf, higherEduDf, countyZipDf);

        institutionPerCountyDf = cleanupDataset(institutionPerCountyDf, higherEduDf, countyZipDf);

        institutionPerCountyDf.show(5);
    }

    private Dataset<Row> loadCensusData() {
        Dataset<Row> censusDf = spark.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("encoding", "cp1252")
                .load("Transformation/src/main/resources/PEP_2017_PEPANNRES.csv");

        censusDf = censusDf
                .drop("GEO.id")
                .drop("rescen42010")
                .drop("resbase42010")
                .drop("respop72010")
                .drop("respop72011")
                .drop("respop72012")
                .drop("respop72013")
                .drop("respop72014")
                .drop("respop72015")
                .drop("respop72016")
                .withColumnRenamed("respop72017", "pop2017")
                .withColumnRenamed("GEO.id2", "countyId")
                .withColumnRenamed("GEO.display-label", "county");

        System.out.println("Complete loading census data");
        return censusDf;
    }

    private Dataset<Row> loadEduData() {
        Dataset<Row> higherEduDf = spark.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("Transformation/src/main/resources/InstitutionCampus.csv");

        higherEduDf = higherEduDf.filter("Locationtype = 'Institution'")
                .withColumn("addressElements", split(higherEduDf.col("Address"), " "))
                .withColumn("addressElementCount", size(col("addressElements")))
                .withColumn("zip9", element_at(col("addressElements"), col("addressElementCount"))) // take last element
                .withColumn("splitZipCode", split(col("zip9"), "-"))
                .withColumn("zip", col("splitZipCode").getItem(0))
                .withColumnRenamed("LocationName", "location")
                .drop("DapipId")
                .drop("OpeId")
                .drop("ParentName")
                .drop("ParentDapipId")
                .drop("zip9")
                .drop("addressElements")
                .drop("addressElementCount")
                .drop("splitZipCode");

        System.out.println("Complete loading edu data");
        return higherEduDf;
    }

    private Dataset<Row> loadCountyZipData() {
        Dataset<Row> countyZipDf = spark.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("Transformation/src/main/resources/county_zip_092018.csv");

        countyZipDf = countyZipDf
                .drop("res_ratio")
                .drop("bus_ratio")
                .drop("oth_ratio")
                .drop("tot_ratio");

        System.out.println("Complete loading county zip");
        return countyZipDf;
    }

    private Dataset<Row> joinDatasets(Dataset<Row> censusDf, Dataset<Row> higherEduDf, Dataset<Row> countyZipDf) {

        Dataset<Row> institutionPerCountyDf =
                higherEduDf.join(countyZipDf, higherEduDf.col("zip").equalTo(countyZipDf.col("zip")), "inner");

        institutionPerCountyDf =
                institutionPerCountyDf.join(censusDf, institutionPerCountyDf.col("county").equalTo(censusDf.col("countyId")), "left");

        System.out.println("Complete joining datasets");
        return institutionPerCountyDf;
    }

    private Dataset<Row> cleanupDataset(Dataset<Row> institutionPerCountyDf, Dataset<Row> higherEduDf, Dataset<Row> countyZipDf) {
        return institutionPerCountyDf
                .drop(higherEduDf.col("zip"))
                .drop(countyZipDf.col("county"))
                .drop("countyId")
                .drop("UpdateDate")
                .drop("Fax")
                .drop("AdminPhone")
                .drop("AdminEmail")
                .distinct();
    }
}
