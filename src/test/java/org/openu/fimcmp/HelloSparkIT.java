package org.openu.fimcmp;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.junit.Ignore;
import org.junit.Test;
import org.openu.fimcmp.algbase.BasicOps;
import scala.Tuple2;

import java.util.*;

/**
 *
 */
@Ignore
public class HelloSparkIT {
    @Test
    public void hello() {
        String inputFile = TestDataLocation.fileStr("README.md");
        String outDir = TestDataLocation.outDir("test-output/word-count");

        SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(inputFile);
        JavaRDD<String> words = input.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        JavaPairRDD<String, Integer> counts = words.
                mapToPair(x -> new Tuple2<>(x, 1)).
                reduceByKey((x, y) -> x + y);
        counts.saveAsTextFile(outDir);
    }

    @Test
    public void computeFIs() {
        String inputFile = TestDataLocation.fileStr("my.small.txt");
        final double minSupport = 0.06;

        SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile(inputFile);

        BasicOps basicOps = new BasicOps();
        JavaRDD<ArrayList<String>> trs = basicOps.linesAsSortedItemsList(lines).persist(StorageLevel.MEMORY_ONLY());
        final long totalTrs = trs.count();
        System.out.println(String.format("%-20s %s", "Transactions count:", totalTrs));
        final long minSuppCount = BasicOps.minSuppCount(totalTrs, minSupport);

        JavaPairRDD<String, Integer> unsortedFis = basicOps.countAndFilterByMinSupport(trs, minSuppCount);
        JavaRDD<Tuple2<String, Integer>> sortedFis = basicOps.sortedByFrequency(unsortedFis, false);
        List<Tuple2<String, Integer>> f1AsList = basicOps.fiRddAsList(sortedFis);

        System.out.println(String.format("%-20s %s", "F1 size:", f1AsList.size()));
        System.out.println(f1AsList);
    }
}
