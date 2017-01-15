package org.openu.fimcmp.eclat;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.openu.fimcmp.FreqItemsetAsRanksBs;
import org.openu.fimcmp.ItemsetAndTids;
import org.openu.fimcmp.ItemsetAndTidsCollection;
import org.openu.fimcmp.util.CountingFakeList;

import java.io.Serializable;
import java.util.*;

/**
 * The main class that implements the Eclat algorithm.
 */
public class EclatAlg implements Serializable {
    private final EclatProperties props;

    public EclatAlg(EclatProperties props) {
        this.props = props;
    }

    public JavaRDD<List<long[]>> computeFreqItemsetsRdd(
            JavaPairRDD<Integer, ItemsetAndTidsCollection> prefRankAndIsAndTidSetRdd) {
        return
                prefRankAndIsAndTidSetRdd.mapValues(this::computeFreqItemsetsSingleDfs)
                .sortByKey()
                .values();
    }

    /**
     * Returns a list of {FI frequency as the first element of the array, FI as a bit set of r1s}. <br/>
     * Use {@link FreqItemsetAsRanksBs#extractItemset(long[])} and {@link FreqItemsetAsRanksBs#extractSupportCnt(long[])}
     * to properly get the FI and its support from the array. <br/>
     */
    @SuppressWarnings("WeakerAccess")
    public List<long[]> computeFreqItemsetsSingleDfs(ItemsetAndTidsCollection initFis) {
        if (initFis.size() <= 1) {
            return Collections.emptyList();
        }

        StatPrinter statPrinter = new StatPrinter(true);

        initFis.sortByKm1Item();
        statPrinter.onStart(initFis);
        LinkedList<ItemsetAndTidsCollection> queue = new LinkedList<>();
        queue.addFirst(initFis);
        List<long[]> res = (props.isCountingOnly) ? new CountingFakeList<>() : new ArrayList<>(10_000);

        while (!queue.isEmpty()) {
            ItemsetAndTidsCollection coll = queue.removeFirst().squeezeIfNeeded(props.isSqueezingEnabled);
            ItemsetAndTids head = coll.popFirst();
            ItemsetAndTidsCollection newColl =
                    coll.joinWithHead(head, res, props.minSuppCount, props.totalFreqItems, props.isUseDiffSets);
            addFirstIfHasPairs(queue, coll);
            addFirstIfHasPairs(queue, newColl);

            statPrinter.onStep(queue, res, head);
        }

        statPrinter.onCompletion(res);
        trimToSize(res);
        return res;
    }

    private static void addFirstIfHasPairs(LinkedList<ItemsetAndTidsCollection> queue, ItemsetAndTidsCollection coll) {
        if (coll.size() > 1) {
            queue.addFirst(coll);
        }
    }

    private static <T> void trimToSize(List<T> res) {
        if (res instanceof ArrayList) {
            ((ArrayList)res).trimToSize();
        }
    }

    private static class StatPrinter {
        private final boolean isEnabled;
        private StopWatch sw;
        private int peakQueueSize;
        private int peakQueueElems;
        private int maxItemsetSize;
        private int iterations;

        StatPrinter(boolean isEnabled) {
            this.isEnabled = isEnabled;
            if (isEnabled) {
                sw = new StopWatch();
            }
        }

        void onStart(ItemsetAndTidsCollection initFis) {
            if (!isEnabled) {
                return;
            }

            sw.start();

            peakQueueSize = initFis.size();
            peakQueueElems = 1;
            maxItemsetSize = initFis.getItemsetSize();
            iterations = 0;

            pp(String.format("Start Eclat: %s elems", initFis.size()));

            if (initFis.size() > 40) {
                ArrayList<ItemsetAndTids> isList = initFis.getObjArrayListCopy();
                for (ItemsetAndTids is : isList) {
                    print(Arrays.toString(is.getItemset()));
                }
            }
        }

        void onStep(List<ItemsetAndTidsCollection> queue, List<?> res, ItemsetAndTids head) {
            if (!isEnabled) {
                return;
            }

            peakQueueSize = Math.max(peakQueueSize, totalElems(queue));
            peakQueueElems = Math.max(peakQueueElems, queue.size());
            maxItemsetSize = Math.max(maxItemsetSize, head.getItemset().length);
            ++iterations;

            if (iterations % 100_000 == 0) {
                pp(String.format(
                        "at: iterations=%s, res elems=%s, queue size=%s (%s elems), head size=%s, " +
                                "peak queue size=%s (%s elems), max itemset size=%s, free mem=%s",
                        iterations, res.size(), totalElems(queue), queue.size(), head.getItemset().length,
                        peakQueueSize, peakQueueElems, maxItemsetSize, Runtime.getRuntime().freeMemory()));
//                System.out.println(String.format("coll[%s]: head: %s\nnewColl[%s]: %s",
//                        coll.size(), head.toString(rankToItem),
//                        newColl.size(), newColl.getFirstAsStringOrNull(rankToItem)));
            }
        }

        void onCompletion(List<?> res) {
            if (!isEnabled) {
                return;
            }

            pp(String.format(
                    "Complete Eclat: iterations=%s, res elems=%s, peak queue size=%s (%s elems), " +
                            "max itemset size=%s, free mem=%s",
                    iterations, res.size(), peakQueueSize, peakQueueElems,
                    maxItemsetSize, Runtime.getRuntime().freeMemory()));
        }

        private static int totalElems(List<ItemsetAndTidsCollection> queue) {
            int res = 0;
            for (ItemsetAndTidsCollection coll : queue) {
                res += coll.size();
            }
            return res;
        }

        private void pp(String msg) {
            print(String.format("%-15s %s", tt(sw), msg));
        }

        private void print(String msg) {
            System.out.println(msg);
        }

        private static String tt(StopWatch sw) {
            return "[" + sw.toString() + "] ";
        }
    }
}
