package org.openu.fimcmp.algs.apriori;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.openu.fimcmp.itemset.FreqItemset;
import org.openu.fimcmp.itemset.FreqItemsetAsRanksBs;
import org.openu.fimcmp.algs.bigfim.BigFimAlgProperties;
import org.openu.fimcmp.util.IteratorOverArray;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * The main class that implements Apriori algorithm.
 */
@SuppressWarnings("WeakerAccess")
public class AprioriAlg<T extends Comparable<T>> implements Serializable {
    private final long minSuppCount;
    private final AprCandidateFisGenerator candidateFisGenerator;

    public static Class[] getClassesToRegister() {
        return new Class[]{
                AprCandidateFisGenerator.class,
                AprioriAlg.class,
                CurrSizeFiRanks.class,
                FiRanksToFromItems.class,
                NextSizeItemsetGenHelper.class,
                PairElem1IteratorOverRankToTidSet.class,
                PairRanks.class,
                TidMergeSet.class,
                TidsGenHelper.class,
        };
    }

    public AprioriAlg(long minSuppCount) {
        this.minSuppCount = minSuppCount;
        this.candidateFisGenerator = new AprCandidateFisGenerator();
    }

    /**
     * @return frequent items sorted by decreasing frequency
     */
    public List<Tuple2<T, Integer>> computeF1WithSupport(JavaRDD<T[]> data) {
        int numParts = data.getNumPartitions();
        Partitioner partitioner = new HashPartitioner(numParts);
        List<Tuple2<T, Integer>> res = data.flatMap(IteratorOverArray::new) //RDD<T>
                .mapToPair(v -> new Tuple2<>(v, 1L))                        //RDD<(T, 1)>
                .reduceByKey(partitioner, (x, y) -> x + y)                  //RDD<(T, sum)>
                .filter(t -> t._2 >= minSuppCount)
                .map(t -> new Tuple2<>(t._1, (int) (long) t._2))              //RDD<(T, int sum)>
                .collect();
        res = new ArrayList<>(res);
        Collections.sort(res, (t1, t2) -> t2._2.compareTo(t1._2));
        return res;
    }

    public List<T> computeF1(JavaRDD<T[]> data) {
        int numParts = data.getNumPartitions();
        Partitioner partitioner = new HashPartitioner(numParts);
        List<Tuple2<T, Long>> res = data.flatMap(IteratorOverArray::new)
                .mapToPair(v -> new Tuple2<>(v, 1L))
                .reduceByKey(partitioner, (x, y) -> x + y)
                .filter(t -> t._2 >= minSuppCount)
                .collect();
        return res.stream()
                .sorted((t1, t2) -> t2._2.compareTo(t1._2))
                .map(t -> t._1).collect(Collectors.toList());
    }

    /**
     * Compute a mapping 2-itemset to count. <br/>
     * The infrequent 2-itemsets are not yet filtered out. <br/>
     * See {@link #countArrToCols(int[][], int)} for details on the returned object
     */
    public List<int[]> computeF2_Part(JavaRDD<int[]> filteredTrs, int totalFreqItems) {
        int[][] candToCount = filteredTrs
                .mapPartitions(trIt -> candidateFisGenerator.countCands2_Part(trIt, totalFreqItems))
                .fold(new int[0][], candidateFisGenerator::mergeCounts_Part);

        return countArrToCols(candToCount, totalFreqItems);
    }

    /**
     * Compute a mapping k-itemset to count. <br/>
     * The infrequent k-itemsets are not yet filtered out. <br/>
     * See {@link #countArrToCols(int[][], int)} for details on the returned object
     */
    public List<int[]> computeFk_Part(
            int k, JavaRDD<Tuple2<int[], long[]>> ranks1AndKm1, NextSizeItemsetGenHelper genHelper) {
//        rangePartitioner = new RangePartitioner(50, pairRdd)
        int[][] candToCount = ranks1AndKm1
                .mapPartitions(trIt -> candidateFisGenerator.countCandsK_Part(trIt, k - 1, genHelper))
                .fold(new int[0][], candidateFisGenerator::mergeCounts_Part);

        final int totalFreqItems = genHelper.getTotalFreqItems();
        return countArrToCols(candToCount, totalFreqItems);
    }

    /**
     * @return (k_itemset to count) mapping as a list of 'columns'. <br/>
     * A 'column' = array whose first element is a frequent item rank, and the rest is a mapping ((k-1)-FI rank to count)
     */
    private List<int[]> countArrToCols(int[][] candToCount, int totalFreqItems) {
        List<int[]> res = new ArrayList<>(totalFreqItems);
        for (int item = 0; item < totalFreqItems; ++item) {
            int[] srcCol = candToCount[item];
            int[] resCol = new int[1 + srcCol.length];
            resCol[0] = item;
            System.arraycopy(srcCol, 0, resCol, 1, srcCol.length);
            res.add(resCol);
        }
        return res;
    }

    public JavaRDD<Tuple2<int[], long[]>> toRddOfRanks1And2(
            JavaRDD<int[]> filteredTrs, CurrSizeFiRanks preprocessedF2) {
        return filteredTrs.map(tr -> candidateFisGenerator.toSortedRanks1AndBitArrayOfRanks2(tr, preprocessedF2));
    }

    public JavaRDD<Tuple2<int[], long[]>> toRddOfRanks1AndK(
            JavaRDD<Tuple2<int[], long[]>> ranks1AndKm1, CurrSizeFiRanks preprocessedFk) {
        return ranks1AndKm1.map(row -> candidateFisGenerator.toSortedRanks1AndBitArrayOfRanksK(row._1, row._2, preprocessedFk));
    }

    /**
     * The core of TID computation
     *
     * @param kRanksBsRdd bit set of k-FI ranks per transaction
     * @return RDD of maps (k-FI rank to bit set of ids of transactions that contain this k-FI). <br/>
     * The 'map' is represented as long[][]. <br/>
     * The fact that the returned object is RDD is not really important as the number of objects in this RDD
     * is expected to be much less than the number of transactions.
     */
    public JavaRDD<long[][]> computeCurrRankToTidBitSet_Part(
            JavaRDD<long[]> kRanksBsRdd, TidsGenHelper tidsGenHelper) {
        JavaPairRDD<long[], Long> kRanksBsWithTidRdd = kRanksBsRdd.zipWithIndex();
        List<Tuple3<Integer, Long, Long>> partIndMinAndMaxTidList =
                kRanksBsWithTidRdd.mapPartitionsWithIndex(TidMergeSet::findMinAndMaxTids, true).collect();
        Map<Integer, Tuple2<Long, Long>> partIndToMinAndMaxTid = tripletListToMap(partIndMinAndMaxTidList);

        return kRanksBsWithTidRdd
                .mapPartitionsWithIndex((partInd, kRanksBsAndTidIt) -> TidMergeSet.processPartition(
                        kRanksBsAndTidIt, tidsGenHelper, partIndToMinAndMaxTid.get(partInd)), true);
    }

    public long[][] mergePartitions(JavaRDD<long[][]> rankToTidBsRdd, TidsGenHelper tidsGenHelper) {
        return rankToTidBsRdd
                .fold(new long[0][], (p1, p2) -> TidMergeSet.mergePartitions(p1, p2, tidsGenHelper));
    }

    /**
     * Get the number of partitions to be used by parallel Eclat. <br/>
     * Ideally, if we use Fk as the base, it should be the number of (k-1)-size prefixes. <br/>
     * But it can't be too large number, so it is bounded by: <ol>
     * <li>(number of machines) * (number of processors)</li>
     * <li>Optional user property (see {@link BigFimAlgProperties#maxEclatNumParts})</li>
     * </ol>
     * The returned number is accompanied by a string explaining how it has been obtained.
     *
     * @param inputNumParts       number of input partitions, usually the number of machines
     * @param rkToRkm1AndR1       rankK -> (rank(k-1), rank1)
     * @param optionalMaxNumParts optional user property
     */
    public static Tuple2<Integer, String> getNumPartsForEclat(
            int inputNumParts, PairRanks rkToRkm1AndR1, Integer optionalMaxNumParts) {
        List<String> numStrs = new ArrayList<>();
        int numRanksKm1 = rkToRkm1AndR1.totalElems1(); //the number of (k-1)-size prefixes
        numStrs.add("prefixes=" + numRanksKm1);

        int numProcs = Runtime.getRuntime().availableProcessors();
        int totalNumProcs = inputNumParts * numProcs;
        numStrs.add(String.format("(%s=%s machines * %s processors)", totalNumProcs, inputNumParts, numProcs));
        int numParts = Math.min(numRanksKm1, totalNumProcs);

        if (optionalMaxNumParts != null) {
            numParts = Math.min(numParts, optionalMaxNumParts);
            numStrs.add("user-prop=" + optionalMaxNumParts);
        }

        String msg = String.format("%s = min(%s)", numParts, StringUtils.join(numStrs, ", "));
        return new Tuple2<>(numParts, msg);
    }

    /**
     * @return RDD of pairs (rank{k-1}, list of its matching TidMergeSet objects)
     */
    public JavaPairRDD<Integer, List<long[]>> groupTidSetsByRankKm1(
            JavaRDD<long[][]> rankToTidBsRdd, PairRanks rkToRkm1AndR1, int resNumParts) {
        IntToSamePartitioner partitioner = new IntToSamePartitioner(resNumParts);
        int totalR1s = rkToRkm1AndR1.totalElems2();
        return rankToTidBsRdd
                .flatMapToPair(kRankToTidSet -> new PairElem1IteratorOverRankToTidSet(kRankToTidSet, rkToRkm1AndR1))
                //aggregate by rank{k-1}:
                .aggregateByKey(new ArrayList<>(totalR1s), partitioner, AprioriAlg::add, AprioriAlg::addAll);
    }

    private static class IntToSamePartitioner extends Partitioner implements Serializable {
        private final int numPartitions;

        private IntToSamePartitioner(int numPartitions) {
            this.numPartitions = numPartitions;
        }

        @Override
        public int numPartitions() {
            return numPartitions;
        }

        @Override
        public int getPartition(Object key) {
            return (Integer) key % numPartitions;
        }
    }

    private static List<long[]> add(List<long[]> acc, long[] elem) {
        acc.add(elem);
        return acc;
    }

    private static List<long[]> addAll(List<long[]> acc, List<long[]> acc2) {
        acc.addAll(acc2);
        return acc;
    }

    private <T1, T2, T3> Map<T1, Tuple2<T2, T3>> tripletListToMap(List<Tuple3<T1, T2, T3>> tripletList) {
        Map<T1, Tuple2<T2, T3>> res = new HashMap<>(tripletList.size() * 2);
        for (Tuple3<T1, T2, T3> triplet : tripletList) {
            res.put(triplet._1(), new Tuple2<>(triplet._2(), triplet._3()));
        }
        return res;
    }

    /**
     * Filter out infrequent candidates and convert 'columns' to triplets (frequent item rank, (k-1)-FI rank, count). <br/>
     * See {@link AprCandidateFisGenerator#fkColToPairs(int[], long)} for details.
     */
    public List<int[]> fkAsArraysToFilteredRankPairs(List<int[]> cols) {
        List<int[]> res = new ArrayList<>(cols.size() * cols.size());
        for (int[] col : cols) {
            res.addAll(candidateFisGenerator.fkColToPairs(col, minSuppCount));
        }
        return res;
    }

    public List<FreqItemset> fkAsArraysToResItemsets(
            List<int[]> cols, String[] rankToItem, FiRanksToFromItems fiRanksToFromItems) {
        List<long[]> resBs = fkAsArraysToItemsetBitsets(cols, fiRanksToFromItems, rankToItem.length);

        List<FreqItemset> res = new ArrayList<>(resBs.size());
        for (long[] ranksWithSuppBs : resBs) {
            int[] ranks = FreqItemsetAsRanksBs.extractItemset(ranksWithSuppBs);
            int freq = FreqItemsetAsRanksBs.extractSupportCnt(ranksWithSuppBs);
            res.add(FreqItemset.constructFromRanks(ranks, freq, rankToItem));
        }

        return res;
    }

    public List<long[]> fkAsArraysToItemsetBitsets(
            List<int[]> cols, FiRanksToFromItems fiRanksToFromItems, int totalFreqItemsets) {
        ArrayList<long[]> res = new ArrayList<>((int) Math.pow(cols.size(), 3));
        final int kk = fiRanksToFromItems.getMaxK() + 1; //no fiRanks object for the maximal rank (k)
        for (int[] col : cols) {
            List<int[]> itemAndPairRanks = candidateFisGenerator.fkColToPairs(col, minSuppCount);
            for (int[] itemAndPairRank : itemAndPairRanks) {
                final int freq = itemAndPairRank[2];
                int[] resItemset = new int[kk];
                resItemset[0] = itemAndPairRank[0];
                int[] itemsetAsR1s = fiRanksToFromItems.getItemsetByRank(itemAndPairRank[1], kk - 1);
                System.arraycopy(itemsetAsR1s, 0, resItemset, 1, itemsetAsR1s.length);

                res.add(FreqItemsetAsRanksBs.toBitSet(freq, resItemset, totalFreqItemsets));
            }
        }

        res.trimToSize();
        return res;
    }
}
