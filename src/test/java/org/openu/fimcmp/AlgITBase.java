package org.openu.fimcmp;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.openu.fimcmp.algs.algbase.BasicOps;

import java.util.ArrayList;

/**
 * Basic class for all integration tests
 */
@SuppressWarnings("WeakerAccess")
public class AlgITBase {
    protected JavaSparkContext sc;
    protected StopWatch sw;
    protected BasicOps basicOps;

    protected static String tt(StopWatch sw) {
        return "[" + sw.toString() + "] ";
    }

    protected void setUpRun(boolean useKryo) throws Exception {
        sc = SparkContextFactory.createSparkContext(useKryo, "local");
        sw = new StopWatch();
        basicOps = new BasicOps();
    }

    protected PrepStepOutputAsList prepareAsList(
            String inputFileName, double minSupport, boolean isPersist, int numPart) {
        String inputFile = TestDataLocation.fileStr(inputFileName);

        if (!sw.isStarted()) {
            sw.start();
        }
        JavaRDD<ArrayList<String>> trs = basicOps.readLinesAsSortedItemsList(inputFile, numPart, sc);
        if (isPersist) {
            trs = trs.persist(StorageLevel.MEMORY_ONLY_SER());
        }
        UsefulCounts cnts = getCounts(trs, minSupport);
        return new PrepStepOutputAsList(trs, cnts.totalTrs, cnts.minSuppCount, cnts.numParts);
    }

    protected PrepStepOutputAsArr prepareAsArr(String inputFileName, double minSupport, boolean isPersist) {
        return prepareAsArr(inputFileName, minSupport, isPersist, 1);
    }

    protected PrepStepOutputAsArr prepareAsArr(
            String inputFileName, double minSupport, boolean isPersist, int numPart) {
        String inputFile = TestDataLocation.fileStr(inputFileName);

        sw.start();
        JavaRDD<String[]> trs = BasicOps.readLinesAsSortedItemsArr(inputFile, numPart, sc);
        if (isPersist) {
            trs = trs.persist(StorageLevel.MEMORY_ONLY_SER());
        }
        UsefulCounts cnts = getCounts(trs, minSupport);
        return new PrepStepOutputAsArr(trs, cnts.totalTrs, cnts.minSuppCount, cnts.numParts);
    }

    private <T> UsefulCounts getCounts(JavaRDD<T> trs, double minSupport) {
        final int numParts = trs.getNumPartitions();
        final long totalTrs = trs.count();
        final long minSuppCount = BasicOps.minSuppCount(totalTrs, minSupport);
        pp("Total records: " + totalTrs);
        pp("Min support: " + minSuppCount);
        pp("Partitions: " + numParts);
        return new UsefulCounts(totalTrs, minSuppCount, numParts);
    }

    protected void pp(Object msg) {
        pp(sw, msg);
    }

    public static void pp(StopWatch sw, Object msg) {
        System.out.println(String.format("%-15s %s", tt(sw), msg));
    }

    private static class UsefulCounts {
        public final long totalTrs;
        public final long minSuppCount;
        public final int numParts;

        UsefulCounts(long totalTrs, long minSuppCount, int numParts) {
            this.totalTrs = totalTrs;
            this.minSuppCount = minSuppCount;
            this.numParts = numParts;
        }
    }

    protected static class PrepStepOutputAsList extends UsefulCounts {
        public final JavaRDD<ArrayList<String>> trs;

        public PrepStepOutputAsList(
                JavaRDD<ArrayList<String>> trs, long totalTrs, long minSuppCount, int numParts) {
            super(totalTrs, minSuppCount, numParts);
            this.trs = trs;
        }
    }

    protected static class PrepStepOutputAsArr extends UsefulCounts {
        public final JavaRDD<String[]> trs;

        public PrepStepOutputAsArr(
                JavaRDD<String[]> trs, long totalTrs, long minSuppCount, int numParts) {
            super(totalTrs, minSuppCount, numParts);
            this.trs = trs;
        }
    }
}
