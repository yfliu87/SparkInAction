package util;

import org.apache.spark.sql.SparkSession;

/**
 * Created by yifeiliu on 9/21/19.
 * Description:
 */
public class SparkContextBuilder {

    public static SparkSession getSparkSession(String appName) {
        return SparkSession.builder().appName(appName).master("local").getOrCreate();
    }
}
