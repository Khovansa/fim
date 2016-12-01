package org.openu.fimcmp.apriori;

import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.openu.fimcmp.BasicOps;
import org.openu.fimcmp.FreqItemset;
import org.openu.fimcmp.util.IteratorOverArray;
import scala.Tuple2;

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

    public AprioriAlg(long minSuppCount) {
        this.minSuppCount = minSuppCount;
        this.candidateFisGenerator = new AprCandidateFisGenerator();
    }

    /**
     * @return frequent items sorted by decreasing frequency
     */
    public List<T> computeF1(JavaRDD<T[]> data) {
        int numParts = data.getNumPartitions();
        HashPartitioner partitioner = new HashPartitioner(numParts);
        List<Tuple2<T, Long>> res = data.flatMap(IteratorOverArray::new)
                .mapToPair(v -> new Tuple2<>(v, 1L))
                .reduceByKey(partitioner, (x, y) -> x + y)
                .filter(t -> t._2 >= minSuppCount)
                .collect();
        return res.stream()
                .sorted((t1, t2) -> t2._2.compareTo(t1._2))
                .map(t -> t._1).collect(Collectors.toList());
    }

    public List<int[]> computeF2_Direct(JavaRDD<int[]> filteredTrs, int totalFreqItems) {
        return filteredTrs
                .flatMap(tr -> new IteratorOverArray<>(candidateFisGenerator.genTransactionC2s_Direct(tr)))
                .mapToPair(elem -> new Tuple2<>(elem[0], elem))
                .aggregateByKey(
                        new int[]{},
                        (col, elem) -> candidateFisGenerator.mergeElem_Direct(totalFreqItems, col, elem),
                        candidateFisGenerator::mergeColumns_Direct)
                .sortByKey()
                .values()
                .collect();
    }

    public JavaRDD<Tuple2<int[], long[]>> toRddOfRanks1And2_BitSet(
            JavaRDD<int[]> filteredTrs, CurrSizeFiRanks preprocessedF2) {
        return filteredTrs.map(tr -> candidateFisGenerator.toSortedRanks1And2_BitSet_Tmp(tr, preprocessedF2));
    }

    public JavaRDD<Tuple2<int[], long[]>> toRddOfRanks1AndK_BitSet(
            JavaRDD<Tuple2<int[], long[]>> ranks1AndKm1, CurrSizeFiRanks preprocessedFk) {
        return ranks1AndKm1.map(row -> candidateFisGenerator.toSortedRanks1AndK_BitSet(row._1, row._2, preprocessedFk));
    }

    public List<int[]> computeFk_BitSet_Direct(
            int k, JavaRDD<Tuple2<int[], long[]>> ranks1AndKm1, NextSizeItemsetGenHelper genHelper) {
        final int totalCands = genHelper.getTotalCurrSizeRanks();
        return ranks1AndKm1
                .flatMap(tr -> new IteratorOverArray<>(
                        candidateFisGenerator.genNextSizeCands_ByItems_BitSet_Direct(k-1, tr, genHelper)))
                .mapToPair(col -> new Tuple2<>(col[0], col))
                .aggregateByKey(
                        new int[]{},
                        (col, elem) -> candidateFisGenerator.mergeElem_Direct(totalCands, col, elem),
                        candidateFisGenerator::mergeColumns_Direct
                )
                .sortByKey()
                .values()
                .collect();
    }

    public long[][] toRddOfTidsNew2D_AllAtOnce2(
            JavaRDD<long[]> tidAndRanksBitset, long totalTids, TidsGenHelper tidsGenHelper) {

        JavaPairRDD<Integer, long[][]> rkToTids = tidAndRanksBitset.mapToPair(t -> new Tuple2<>(1, t))
                .aggregateByKey(
                        new long[][]{},
                        1,
                        (res, elem) -> TidMergeSetNew.mergeElem2D_AllAtOnce2(res, elem, totalTids, tidsGenHelper),
                        TidMergeSetNew::mergeSets2D
                );
        return rkToTids.values().collect().get(0);
    //                .filter(arr -> (arr != null))
    }

    public JavaRDD<long[]> prepareToTidsGen2D_AllAtOnce_BitSet2(
            JavaRDD<Tuple2<int[], long[]>> ranks1AndK, TidsGenHelper tidsGenHelper) {
        return ranks1AndK
                .zipWithIndex()
                .map(trAndTid -> candidateFisGenerator
                        .getRankToTidNew2D_AllAtOnce_BitSet2(trAndTid._1._2, trAndTid._2, tidsGenHelper));
    }

    public List<int[]> fkAsArraysToRankPairs_Direct(List<int[]> cols, long minSuppCount) {
        List<int[]> res = new ArrayList<>(cols.size() * cols.size());
        for (int[] col : cols) {
            res.addAll(candidateFisGenerator.fkColToPairs_Direct(col, minSuppCount));
        }
        return res;
    }

    public List<FreqItemset<String>> fkAsArraysToResItemsets_Direct(
            long minSuppCount, List<int[]> cols, Map<String, Integer> itemToRank, CurrSizeFiRanks... fkToF2RanksArr) {
        String[] rankToItem = BasicOps.getRankToItem(itemToRank);

        List<FreqItemset<String>> res = new ArrayList<>((int) Math.pow(cols.size(), 3));
        for (int[] col : cols) {
            List<int[]> itemAndPairRanks = candidateFisGenerator.fkColToPairs_Direct(col, minSuppCount);
            for (int[] itemAndPairRank : itemAndPairRanks) {
                ArrayList<String> resItemset = new ArrayList<>();
                final int freq = itemAndPairRank[2];
                resItemset.add(rankToItem[itemAndPairRank[0]]);

                //currRank is a rank of (k-1)-FI, which in case of k=2 means an item rank
                int currRank = itemAndPairRank[1];
                for (CurrSizeFiRanks fiRanks : fkToF2RanksArr) {
                    int[] pair = fiRanks.getCurrSizeFiAsPairByRank(currRank);
                    resItemset.add(rankToItem[pair[0]]); //the first element of a pair is always an item
                    currRank = pair[1]; //(i-1)-FI rank
                }
                resItemset.add(rankToItem[currRank]);

                resItemset.trimToSize();
                res.add(new FreqItemset<>(resItemset, freq));
            }
        }

        return res;
    }
}
