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

    protected PrepStepOutput prepare(String inputFileName, double minSupport, boolean isPersist) {
        String inputFile = TestDataLocation.fileStr(inputFileName);

        sw.start();
        JavaRDD<ArrayList<String>> trs = basicOps.readLinesAsSortedItems(inputFile, sc);
        if (isPersist) {
            trs = trs.persist(StorageLevel.MEMORY_ONLY_SER());
        }
        final long totalTrs = trs.count();
        final long minSuppCount = BasicOps.minSuppCount(totalTrs, minSupport);
        pp("Total records: " + totalTrs);
        pp("Min support: " + minSuppCount);

        return new PrepStepOutput(trs, totalTrs, minSuppCount);
    }

    protected PrepStepOutputNew prepareNew(String inputFileName, double minSupport, boolean isPersist) {
        String inputFile = TestDataLocation.fileStr(inputFileName);

        sw.start();
        JavaRDD<String[]> trs = basicOps.readLinesAsSortedItemsNew(inputFile, sc);
        if (isPersist) {
            trs = trs.persist(StorageLevel.MEMORY_ONLY_SER());
        }
        final long totalTrs = trs.count();
        final long minSuppCount = BasicOps.minSuppCount(totalTrs, minSupport);
        pp("Total records: " + totalTrs);
        pp("Min support: " + minSuppCount);

        return new PrepStepOutputNew(trs, totalTrs, minSuppCount);
    }

    protected void pp(Object msg) {
        System.out.println(String.format("%-15s %s", tt(sw), msg));
    }

    protected static class PrepStepOutput {
        public final JavaRDD<ArrayList<String>> trs;
        public final long totalTrs;
        public final long minSuppCount;

        public PrepStepOutput(
                JavaRDD<ArrayList<String>> trs, long totalTrs, long minSuppCount) {
            this.trs = trs;
            this.totalTrs = totalTrs;
            this.minSuppCount = minSuppCount;
        }
    }

    protected static class PrepStepOutputNew {
        public final JavaRDD<String[]> trs;
        public final long totalTrs;
        public final long minSuppCount;

        public PrepStepOutputNew(
                JavaRDD<String[]> trs, long totalTrs, long minSuppCount) {
            this.trs = trs;
            this.totalTrs = totalTrs;
            this.minSuppCount = minSuppCount;
        }
    }
}
