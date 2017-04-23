package org.openu.fimcmp.bigfim;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import org.openu.fimcmp.SparkContextFactory;
import org.openu.fimcmp.TestDataLocation;

import static org.openu.fimcmp.AlgITBase.pp;

/**
 * Runner of BigFimAlf
 */
public class BigFimAlgIT {
    public static void main(String[] args) throws Exception {
        BigFimAlgIT test = new BigFimAlgIT();
        test.run();
    }

    @Test
    public void run() throws Exception {
        final double minSupp = 0.8;
//        final String inputFileName = "my.small.txt";
        final String inputFileName = "pumsb.dat";
        final int prefixLenToStartEclat = 3;
        BigFimAlgProperties props = new BigFimAlgProperties(minSupp, prefixLenToStartEclat);
        props.maxEclatNumParts = 3;
        BigFimAlg alg = new BigFimAlg(props);

        StopWatch sw = new StopWatch();
        sw.start();
        pp(sw, "Starting the Spark context");
        boolean isUseKrio = !props.isCountingOnly;
        JavaSparkContext sc = SparkContextFactory.createSparkContext(isUseKrio, "local");
        pp(sw, "Completed starting the Spark context");
        Thread.sleep(1_200_000L);

        sw.stop();
        sw.reset();
        sw.start();
        String inputFile = TestDataLocation.fileStr(inputFileName);
        JavaRDD<String[]> trs = alg.readInput(sc, inputFile, sw);

        BigFimResult res = alg.computeFis(trs, sw);
        res.printCounts(sw);
    }
}
