package ingestionFromDatabase;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

import static util.SparkContextBuilder.getSparkSession;

/**
 * Created by yifeiliu on 9/21/19.
 * Description:
 */
public class MySQLToDataframeApp {

    private static SparkSession spark;

    public static void main(String[] args) {
        new MySQLToDataframeApp().start();
    }

    private void start() {

        spark = getSparkSession("MySQL to dataframe");

        loadData();

        filterData();
    }

    private void loadData() {
        // option1
        Dataset<Row> df = option1();
        df.orderBy(df.col("last_name"));
        df.show(5);
        df.printSchema();

        df = option2();
        df.orderBy(df.col("last_name"));
        df.show(5);
        df.printSchema();

        df = customizePartition();
        df.show();
        System.out.println("The dataframe is split over " + df.rdd().getPartitions().length + " partition(s).");
    }

    private Dataset<Row> option1() {
        String url = "";
        String table = "";
        Properties prop = new Properties();
        prop.put("user", "root");
        prop.put("password", "");
        prop.put("useSSL", "false");

        return spark.read().jdbc(url, table, prop);
    }

    private Dataset<Row> option2() {
        return spark.read()
                .option("url", "")
                .option("dbtable", "")
                .option("user", "root")
                .option("password", "")
                .option("useSSL", "false")
                .option("serverTimeZone", "PST")
                .format("jdbc")
                .load();
    }

    private Dataset<Row> customizePartition() {
        Properties prop = new Properties();
        prop.put("user", "root");
        prop.put("password", "");
        prop.put("useSSL", "false");
        prop.put("serverTimezone", "PST");

        prop.put("partitionColumn", "film_id");
        prop.put("lowerBound", "1");
        prop.put("upperBound", "1000");
        prop.put("numPartitions", "10");

        return spark.read().jdbc("jdbc:mysql://localhost:3306/sakila", "film", prop);
    }

    private void filterData() {
        Properties prop = new Properties();
        prop.put("user", "root");
        prop.put("password", "");
        prop.put("useSSL", "false");
        prop.put("serverTimezone", "PST");

        String sqlQuery = "select * from film where "
                + "(title like \"%ALIEN%\" or title like \"%victory%\" "
                + "or title like \"%agent%\" or description like \"%action%\") "
                + "and rental_rate>1 "
                + "and (rating=\"G\" or rating=\"PG\")";

        Dataset<Row> df = spark.read()
                .jdbc("jdbc:mysql://localhost:3306/sakila", "(" + sqlQuery + ") film_alias", prop);

        df.show();
        df.printSchema();
    }

    private void joinData() {
        Properties prop = new Properties();
        prop.put("user", "root");
        prop.put("password", "");
        prop.put("useSSL", "false");
        prop.put("serverTimezone", "PST");

        String sqlQuery = "select actor.first_name, actor.last_name, film.title, film.description "
                + "from actor, film_actor, film "
                + "where actor.actor_id = film_actor.actor_id "
                + "and film_actor.film_id = film.film_id";

        Dataset<Row> df = spark.read()
                .jdbc("jdbc:mysql://localhost:3306/sakila", "(" + sqlQuery + ") actor_film_alias", prop);

        df.show();
        df.printSchema();
    }
}
