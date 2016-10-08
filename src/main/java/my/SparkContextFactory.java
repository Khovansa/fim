package my;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Utility class to create the JavaSparkContext
 */
public class SparkContextFactory {
    public static JavaSparkContext createLocalSparkContext() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("FI Comparison");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        return sc;
    }
}
