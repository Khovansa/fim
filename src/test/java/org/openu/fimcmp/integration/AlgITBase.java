package org.openu.fimcmp.integration;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.openu.fimcmp.SparkContextFactory;
import org.openu.fimcmp.algs.algbase.BasicOps;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Basic class for all integration tests
 */
@SuppressWarnings("WeakerAccess")
public class AlgITBase {
    private static final String TEST_DATA_DIR_SYSTEM_PROP_NAME = "TEST_DATA_DIR";

    protected Path dataDir;
    protected JavaSparkContext sc;
    protected StopWatch sw;
    protected BasicOps basicOps;

    protected void setUpRun(boolean useKryo) throws Exception {
        String testDataDirProp = System.getProperty(TEST_DATA_DIR_SYSTEM_PROP_NAME);
        dataDir = (testDataDirProp != null) ? Paths.get(testDataDirProp) : null;

        sc = SparkContextFactory.createSparkContext(useKryo, "local");
        sw = new StopWatch();
        basicOps = new BasicOps();
    }

    protected PrepStepOutputAsArr prepareAsArr(
            String inputFileName, double minSupport, boolean isPersist, int numPart) {
        String inputFile = fileStr(inputFileName);

        sw.start();
        JavaRDD<String[]> trs = BasicOps.readLinesAsSortedItemsArr(inputFile, numPart, sc);
        if (isPersist) {
            trs = trs.persist(StorageLevel.MEMORY_ONLY_SER());
        }
        UsefulCounts cnts = getCounts(trs, minSupport);
        return new PrepStepOutputAsArr(trs, cnts.totalTrs, cnts.minSuppCount, cnts.numParts);
    }

    protected void pp(Object msg) {
        pp(sw, msg);
    }

    protected static void pp(StopWatch sw, Object msg) {
        System.out.println(String.format("%-15s %s", tt(sw), msg));
    }

    protected static String tt(StopWatch sw) {
        return "[" + sw.toString() + "] ";
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

    private String fileStr(String fileName) {
        return file(fileName).toString();
    }

    private Path file(String fileName) {
        Path res = Paths.get(fileName);

        if (!res.isAbsolute()) {
            if (dataDir == null) {
                String msg = String.format("Input file '%s' is relative and no '%s' system property is defined",
                        fileName, TEST_DATA_DIR_SYSTEM_PROP_NAME);
                throw new IllegalArgumentException(msg);
            }
            res = dataDir.resolve(res);
        }

        if (!res.toFile().exists()) {
            throw new IllegalArgumentException(String.format("File '%s' does not exist", res));
        }

        return res;
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

    protected static class PrepStepOutputAsArr extends UsefulCounts {
        public final JavaRDD<String[]> trs;

        public PrepStepOutputAsArr(
                JavaRDD<String[]> trs, long totalTrs, long minSuppCount, int numParts) {
            super(totalTrs, minSuppCount, numParts);
            this.trs = trs;
        }
    }
}
