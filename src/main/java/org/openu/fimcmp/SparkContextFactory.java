package org.openu.fimcmp;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.openu.fimcmp.algs.algbase.AlgBase;
import org.openu.fimcmp.algs.apriori.AprioriAlg;
import org.openu.fimcmp.algs.bigfim.BigFimAlg;
import org.openu.fimcmp.algs.eclat.EclatAlg;
import org.openu.fimcmp.algs.fin.FinAlg;
import org.openu.fimcmp.algs.fpgrowth.FpGrowthAlg;
import org.openu.fimcmp.itemset.*;
import org.openu.fimcmp.result.*;
import org.openu.fimcmp.util.*;
import scala.Tuple2;

import java.util.*;

/**
 * Utility class to create the JavaSparkContext
 */
@SuppressWarnings("WeakerAccess")
public class SparkContextFactory {
    public static JavaSparkContext createSparkContext(boolean useKryo, String sparkMasterUrl) {
        SparkConf conf = new SparkConf().setAppName("FI Comparison");
//        conf.setMaster("local");
//        conf.setMaster("spark://192.168.1.68:7077");
        if (!StringUtils.isBlank(sparkMasterUrl)) {
            conf.setMaster(sparkMasterUrl);
        }

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
                    FreqItemset.class, ItemsetAndTidsCollection.class, ItemsetAndTids.class, ItemsetAndTids[].class,

                    FiResultHolder.class, BitsetFiResultHolder.class, CountingOnlyFiResultHolder.class,
                    FiResultHolderFactory.class, BitsetFiResultHolderFactory.class,
                    CountingOnlyFiResultHolderFactory.class,

                    BitSet.class, IteratorOverArray.class,

                    Object[].class, boolean[].class,
                    String.class, String[].class, Integer.class, Integer[].class, Integer[][].class, Integer[][][].class,
                    int[].class, int[][].class, int[][][].class, long[].class,
                    ArrayList.class, new ArrayList<>().iterator().getClass(),
                    LinkedList.class, new LinkedList<>().iterator().getClass(),
                    HashSet.class, TreeSet.class, HashMap.class,
                    Tuple2.class, Tuple2[].class,
            });

            conf.registerKryoClasses(AlgBase.getClassesToRegister());
            conf.registerKryoClasses(FinAlg.getClassesToRegister());
            conf.registerKryoClasses(BigFimAlg.getClassesToRegister());
            conf.registerKryoClasses(AprioriAlg.getClassesToRegister());
            conf.registerKryoClasses(EclatAlg.getClassesToRegister());
            conf.registerKryoClasses(FpGrowthAlg.getClassesToRegister());
        }

//        conf.set("spark.driver.memory", "1200m");

        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.setLogLevel("WARN");

        return sc;
    }
}
