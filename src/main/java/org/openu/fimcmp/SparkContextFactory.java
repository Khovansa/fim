package org.openu.fimcmp;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.openu.fimcmp.apriori.*;
import org.openu.fimcmp.eclat.EclatAlg;
import org.openu.fimcmp.util.IteratorOverArray;
import scala.Tuple2;

import java.util.*;

/**
 * Utility class to create the JavaSparkContext
 */
@SuppressWarnings("WeakerAccess")
public class SparkContextFactory {
    public static JavaSparkContext createLocalSparkContext(boolean useKryo) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("FI Comparison");

        conf.set("spark.rdd.compress", "true");

        if (!useKryo) {
            conf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
        } else {
            conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//        conf.set("spark.kryo.registrationRequired", "true");
            conf.set("spark.kryoserializer.buffer", "100m");
            conf.set("spark.kryoserializer.buffer.max", "256m");
//        conf.set("spark.reducer.maxSizeInFlight", "48m"); //500K worked a bit better
            conf.registerKryoClasses(new Class[]{
                    org.apache.spark.mllib.fpm.FPTree.class, org.apache.spark.mllib.fpm.FPTree.Node.class,
                /*org.apache.spark.mllib.fpm.FPTree.Summary.class, */
                    scala.collection.mutable.ListBuffer.class,
                    org.apache.spark.mllib.fpm.FPGrowth.FreqItemset.class,
                    org.apache.spark.mllib.fpm.FPGrowth.FreqItemset[].class,
                    Object[].class,

                    BasicOps.class,
                    HashSet.class, TreeSet.class, HashMap.class, ArrayList.class, LinkedList.class,
                    String.class, String[].class, Integer.class, Integer[].class, Integer[][].class, Integer[][][].class,
                    long[].class, int[].class, int[][].class, int[][][].class, BitSet.class,
                    Tuple2.class, Tuple2[].class,
                    new ArrayList<>().iterator().getClass(), new LinkedList<>().iterator().getClass(),
                    AprCandidateFisGenerator.class, AprioriAlg.class, IteratorOverArray.class, PairRanks.class,
                    TidsGenHelper.class, boolean[].class,
                    TidMergeSet.class, FiRanksToFromItems.class, PairElem1IteratorOverRankToTidSet.class,
                    NextSizeItemsetGenHelper.class,
                    ItemsetAndTidsCollection.class, ItemsetAndTids.class, ItemsetAndTids[].class,

                    EclatAlg.class

            });
        }

        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.setLogLevel("WARN");

        return sc;
    }
}
