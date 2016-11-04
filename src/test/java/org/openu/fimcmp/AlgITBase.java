package org.openu.fimcmp;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.junit.Before;

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

    @Before
    public void setUp() throws Exception {
        sc = SparkContextFactory.createLocalSparkContext();
        sw = new StopWatch();
        basicOps = new BasicOps();
    }

    protected PrepStepOutputAsList prepareAsList(String inputFileName, double minSupport, boolean isPersist) {
        String inputFile = TestDataLocation.fileStr(inputFileName);

        sw.start();
        JavaRDD<ArrayList<String>> trs = basicOps.readLinesAsSortedItemsList(inputFile, sc);
        if (isPersist) {
            trs = trs.persist(StorageLevel.MEMORY_ONLY_SER());
        }
        UsefulCounts cnts = getCounts(trs, minSupport);
        return new PrepStepOutputAsList(trs, cnts.totalTrs, cnts.minSuppCount);
    }

    protected PrepStepOutputAsArr prepareAsArr(String inputFileName, double minSupport, boolean isPersist) {
        String inputFile = TestDataLocation.fileStr(inputFileName);

        sw.start();
        JavaRDD<String[]> trs = basicOps.readLinesAsSortedItemsArr(inputFile, sc);
        if (isPersist) {
            trs = trs.persist(StorageLevel.MEMORY_ONLY_SER());
        }
        UsefulCounts cnts = getCounts(trs, minSupport);
        return new PrepStepOutputAsArr(trs, cnts.totalTrs, cnts.minSuppCount);
    }

    private <T> UsefulCounts getCounts(JavaRDD<T> trs, double minSupport) {
        final long totalTrs = trs.count();
        final long minSuppCount = BasicOps.minSuppCount(totalTrs, minSupport);
        pp("Total records: " + totalTrs);
        pp("Min support: " + minSuppCount);
        return new UsefulCounts(totalTrs, minSuppCount);
    }

    protected void pp(Object msg) {
        System.out.println(String.format("%-15s %s", tt(sw), msg));
    }

    private static class UsefulCounts {
        public final long totalTrs;
        public final long minSuppCount;

        UsefulCounts(long totalTrs, long minSuppCount) {
            this.totalTrs = totalTrs;
            this.minSuppCount = minSuppCount;
        }
    }

    protected static class PrepStepOutputAsList extends UsefulCounts {
        public final JavaRDD<ArrayList<String>> trs;

        public PrepStepOutputAsList(
                JavaRDD<ArrayList<String>> trs, long totalTrs, long minSuppCount) {
            super(totalTrs, minSuppCount);
            this.trs = trs;
        }
    }

    protected static class PrepStepOutputAsArr extends UsefulCounts {
        public final JavaRDD<String[]> trs;

        public PrepStepOutputAsArr(
                JavaRDD<String[]> trs, long totalTrs, long minSuppCount) {
            super(totalTrs, minSuppCount);
            this.trs = trs;
        }
    }
}
