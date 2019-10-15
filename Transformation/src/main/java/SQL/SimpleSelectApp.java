package SQL;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.SparkContextBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by yifeiliu on 9/29/19.
 * Description:
 */
public class SimpleSelectApp {

    private static Logger logger = LoggerFactory.getLogger(SimpleSelectApp.class);

    public static void main(String[] args) {
        new SimpleSelectApp().start();
    }

    private void start() {
        SparkSession session = SparkContextBuilder.getSparkSession("SQL Select App");

        simpleSQL(session);

        simpleSQLWithDataframeAPI(session);
    }

    private void simpleSQL(SparkSession session) {
        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("geo", DataTypes.StringType, true),
                DataTypes.createStructField("yr1980", DataTypes.DoubleType, false)
        });

        Dataset<Row> df = session.read().format("csv")
                .option("header", true)
                .schema(schema)
                .load("Transformation/src/main/resources/populationByCountry19802010.csv");

        df.createOrReplaceGlobalTempView("geodata");
        df.printSchema();

        Dataset<Row> smallCountries = session.sql("select * from global_temp.geodata where yr1980 < 1 order by 2 limit 5");
        smallCountries.show(10, false);

        SparkSession newSession = session.newSession();
        Dataset<Row> bigCountries = newSession.sql("select * from global_temp.geodata where yr1980 >= 1 order by 2 limit 5");
        bigCountries.show(10, false);

    }

    private void simpleSQLWithDataframeAPI(SparkSession session) {
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("geo", DataTypes.StringType, true));

        for (int year = 1980; year <= 2010; year++) {
            fields.add(DataTypes.createStructField("yr" + year, DataTypes.DoubleType, false));
        }

        StructType schema = DataTypes.createStructType(fields.toArray(new StructField[0]));

        Dataset<Row> df = session.read().format("csv")
                .option("header", true)
                .schema(schema)
                .load("Transformation/src/main/resources/populationByCountry19802010.csv");
        df.printSchema();

        for (int i = 1981; i < 2010; i++) {
            df = df.drop(df.col("yr" + i));
        }

        df = df.withColumn("evolution", functions.expr("round((yr2010 - yr1980) * 1000000)"));
        df.printSchema();

        df.createOrReplaceTempView("geodata");

        Dataset<Row> negativeEvolutionDf = session.sql("select * from geodata where geo is not null and evolution <= 0 order by evolution limit 25");
        negativeEvolutionDf.show(15);

        Dataset<Row> moreThanOneMillionEvolutionDf = session.sql("select * from geodata where geo is not null and evolution > 999999 order by evolution desc limit 25");
        moreThanOneMillionEvolutionDf.show(15);
    }
}
