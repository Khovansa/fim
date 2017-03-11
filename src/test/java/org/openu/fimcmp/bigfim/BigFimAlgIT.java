package org.openu.fimcmp.bigfim;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import org.openu.fimcmp.SparkContextFactory;
import org.openu.fimcmp.TestDataLocation;

/**
 * Runner of BigFimAlf
 */
public class BigFimAlgIT {
    @Test
    public void run() {
        final double minSupp = 0.8;
//        final String inputFileName = "my.small.txt";
        final String inputFileName = "pumsb.dat";
        final int prefixLenToStartEclat = 3;
        BigFimProperties props = new BigFimProperties(minSupp, prefixLenToStartEclat);
        props.maxEclatNumParts = 3;
        BigFimAlg alg = new BigFimAlg(props);

        JavaSparkContext sc = SparkContextFactory.createLocalSparkContext(props.isUseKrio());
        StopWatch sw = new StopWatch();

        sw.start();
        String inputFile = TestDataLocation.fileStr(inputFileName);
        JavaRDD<String[]> trs = alg.readInput(sc, inputFile);

        BigFimResult res = alg.computeFis(trs, sw);
        res.printCounts(sw);
    }
}
