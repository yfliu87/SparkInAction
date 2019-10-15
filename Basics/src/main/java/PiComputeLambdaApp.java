import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by yifeiliu on 9/15/19.
 * Description:
 */
public class PiComputeLambdaApp {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        new PiComputeLambdaApp().start(10);
    }

    private void start(int slices) {

        int numberOfThrows = 100000 * slices;

        SparkSession spark = SparkSession
                .builder()
                .appName("Spark Pi with lambdas")
                .master("local[*]")
                .getOrCreate();

        List<Integer> dartThrows = IntStream.range(1, numberOfThrows).boxed().collect(Collectors.toList());

        int count = spark.createDataset(dartThrows, Encoders.INT())
                .javaRDD()
                .map(i -> {
                    double x = Math.random() * 2 - 1;
                    double y = Math.random() * 2 - 1;
                    return (x * x + y * y > 1) ? 0 : 1;
                })
                .reduce((x,y) -> x + y);

        System.out.println("pi: " + (4.0 * count)/numberOfThrows);
    }
}
