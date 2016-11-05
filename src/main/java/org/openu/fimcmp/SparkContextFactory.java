package org.openu.fimcmp;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.openu.fimcmp.apriori.AprCandidateFisGenerator2;
import org.openu.fimcmp.apriori.AprioriAlg2;
import org.openu.fimcmp.util.BoundedIntPairSet;
import org.openu.fimcmp.util.IteratorOverArray;
import scala.Tuple2;

import java.util.*;

/**
 * Utility class to create the JavaSparkContext
 */
@SuppressWarnings("WeakerAccess")
public class SparkContextFactory {
    public static JavaSparkContext createLocalSparkContext() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("FI Comparison");

        conf.set("spark.rdd.compress", "true");

        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//        conf.set("spark.kryo.registrationRequired", "true");
        conf.set("spark.kryoserializer.buffer", "100m");
        conf.set("spark.kryoserializer.buffer.max", "256m");
        conf.registerKryoClasses(new Class[]{
                org.apache.spark.mllib.fpm.FPTree.class, org.apache.spark.mllib.fpm.FPTree.Node.class,
                /*org.apache.spark.mllib.fpm.FPTree.Summary.class, */
                scala.collection.mutable.ListBuffer.class,
                org.apache.spark.mllib.fpm.FPGrowth.FreqItemset.class,
                org.apache.spark.mllib.fpm.FPGrowth.FreqItemset[].class,
                Object[].class,

                BasicOps.class,
                HashSet.class, TreeSet.class, HashMap.class, ArrayList.class,
                String.class, String[].class, Integer.class, Integer[].class, Integer[][].class, int[].class,
                long[].class, int[].class, int[][].class, BitSet.class, BoundedIntPairSet.class,
                Tuple2.class, Tuple2[].class,
                new ArrayList<>().iterator().getClass(),
                AprCandidateFisGenerator2.class, AprioriAlg2.class, IteratorOverArray.class,
        });

        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.setLogLevel("WARN");

        return sc;
    }
}
