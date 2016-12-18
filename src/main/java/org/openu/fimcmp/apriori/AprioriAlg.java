package org.openu.fimcmp.apriori;

import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.openu.fimcmp.BasicOps;
import org.openu.fimcmp.FreqItemset;
import org.openu.fimcmp.util.IteratorOverArray;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The main class that implements Apriori algorithm.
 */
@SuppressWarnings("WeakerAccess")
public class AprioriAlg<T extends Comparable<T>> implements Serializable {
    private final long minSuppCount;
    private final AprCandidateFisGenerator candidateFisGenerator;

    public AprioriAlg(long minSuppCount) {
        this.minSuppCount = minSuppCount;
        this.candidateFisGenerator = new AprCandidateFisGenerator();
    }

    /**
     * @return frequent items sorted by decreasing frequency
     */
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

    public List<int[]> computeF2_Part(JavaRDD<int[]> filteredTrs, int totalFreqItems) {
        int[][] candToCount = filteredTrs
                .mapPartitions(trIt -> candidateFisGenerator.countCands2_Part(trIt, totalFreqItems))
                .fold(new int[0][], candidateFisGenerator::mergeCounts_Part);

        return countArrToCols(candToCount, totalFreqItems);
    }

    public List<int[]> computeFk_Part(
            int k, JavaRDD<Tuple2<int[], long[]>> ranks1AndKm1, NextSizeItemsetGenHelper genHelper) {
//        rangePartitioner = new RangePartitioner(50, pairRdd)
        int[][] candToCount = ranks1AndKm1
                .mapPartitions(trIt -> candidateFisGenerator.countCandsK_Part(trIt, k - 1, genHelper))
                .fold(new int[0][], candidateFisGenerator::mergeCounts_Part);

        final int totalFreqItems = genHelper.getTotalFreqItems();
        return countArrToCols(candToCount, totalFreqItems);
    }

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
        return filteredTrs.map(tr -> candidateFisGenerator.toSortedRanks1And2(tr, preprocessedF2));
    }

    public JavaRDD<Tuple2<int[], long[]>> toRddOfRanks1AndK(
            JavaRDD<Tuple2<int[], long[]>> ranks1AndKm1, CurrSizeFiRanks preprocessedFk) {
        return ranks1AndKm1.map(row -> candidateFisGenerator.toSortedRanks1AndK(row._1, row._2, preprocessedFk));
    }

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

    public JavaPairRDD<Integer, List<long[]>> groupTidSetsByRankKm1(
            JavaRDD<long[][]> rankToTidBsRdd, PairRanks rkToRkm1AndR1) {
//        final int numParts = rankToTidBsRdd.getNumPartitions();
        final int numParts = Math.min(rkToRkm1AndR1.totalElems1(), 8 * rankToTidBsRdd.getNumPartitions());
        IntToSamePartitioner partitioner = new IntToSamePartitioner(numParts);
        int totalR1s = rkToRkm1AndR1.totalElems2();
        return rankToTidBsRdd
                .flatMapToPair(kRankToTidSet -> new PairElem1IteratorOverRankToTidSet(kRankToTidSet, rkToRkm1AndR1))
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
            return (Integer)key % numPartitions;
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

    public List<int[]> fkAsArraysToRankPairs(List<int[]> cols, long minSuppCount) {
        List<int[]> res = new ArrayList<>(cols.size() * cols.size());
        for (int[] col : cols) {
            res.addAll(candidateFisGenerator.fkColToPairs(col, minSuppCount));
        }
        return res;
    }

    public List<FreqItemset<String>> fkAsArraysToResItemsets(
            long minSuppCount, List<int[]> cols, Map<String, Integer> itemToRank, FiRanksToFromItems fiRanksToFromItems) {
        String[] rankToItem = BasicOps.getRankToItem(itemToRank);

        List<FreqItemset<String>> res = new ArrayList<>((int) Math.pow(cols.size(), 3));
        final int kk = fiRanksToFromItems.getMaxK() + 1; //no fiRanks object for the maximal rank (k)
        for (int[] col : cols) {
            List<int[]> itemAndPairRanks = candidateFisGenerator.fkColToPairs(col, minSuppCount);
            for (int[] itemAndPairRank : itemAndPairRanks) {
                final int freq = itemAndPairRank[2];
                ArrayList<String> resItemset = new ArrayList<>(kk);
                resItemset.add(rankToItem[itemAndPairRank[0]]);
                fiRanksToFromItems.addOrigItemsetByRank(resItemset, itemAndPairRank[1], kk-1, rankToItem);

                res.add(new FreqItemset<>(resItemset, freq));
            }
        }

        return res;
    }
}
