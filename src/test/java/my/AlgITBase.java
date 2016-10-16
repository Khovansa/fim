package my;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.junit.Before;
import scala.Tuple2;

import java.util.*;

/**
 * Basic class for all integration tests
 */
@SuppressWarnings("WeakerAccess")
public class AlgITBase {
    protected JavaSparkContext sc;
    protected StopWatch sw;
    protected BasicOps basicOps;
    protected ListComparator<String> listComparator;

    protected static String tt(StopWatch sw) {
        return "[" + sw.toString() + "] ";
    }

    @Before
    public void setUp() throws Exception {
        sc = SparkContextFactory.createLocalSparkContext();
        sw = new StopWatch();
        basicOps = new BasicOps();
        listComparator = new ListComparator<>();
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

    protected void exploreF1(JavaPairRDD<String, Integer> f1AsRdd) {
        SortedSet<String> f1AsSet = basicOps.fillCollectionFromRdd(new TreeSet<>(), f1AsRdd);
        pp("F1 size = " + f1AsSet.size());
        pp(f1AsSet);
        JavaRDD<Tuple2<String, Integer>> sortedF1 = basicOps.sortedByFrequency(f1AsRdd, false);
        List<Tuple2<String, Integer>> f1AsList = basicOps.fiRddAsList(sortedF1);
        pp(f1AsList);
    }

    protected void exploreFk(JavaPairRDD<List<String>, Integer> fkAsRdd, Collection<List<String>> fk, int k) {
        pp(String.format("F%s size = %s", k, fk.size()));
        pp(new ArrayList<>(fk).subList(0, Math.min(fk.size(), 200)));
        JavaRDD<Tuple2<List<String>, Integer>> sortedFk =
                basicOps.sortedByFrequency(fkAsRdd, listComparator, false);
        List<Tuple2<List<String>, Integer>> fkAsTupleList = basicOps.fiRddAsList(sortedFk);
        pp(fkAsTupleList.subList(0, Math.min(fkAsTupleList.size(), 200)));
    }

    protected String laToString(List<Integer[]> listOfArrays, Integer optMaxArrays) {
        int maxArrays = (optMaxArrays != null) ? Math.min(optMaxArrays, listOfArrays.size()) : listOfArrays.size();
        List<String> outList = new ArrayList<>(maxArrays);
        for (Integer[] arr : listOfArrays.subList(0, maxArrays)) {
            outList.add(Arrays.toString(arr));
        }
        return outList.toString();
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
