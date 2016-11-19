package org.openu.fimcmp.apriori;

import org.apache.spark.HashPartitioner;
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

    public List<int[]> computeF2(JavaRDD<int[]> filteredTrs) {
        return filteredTrs
                .flatMap(tr -> new IteratorOverArray<>(candidateFisGenerator.genTransactionC2s(tr)))
                .mapToPair(col -> new Tuple2<>(col[0], col))
                .foldByKey(new int[]{}, candidateFisGenerator::mergeColumns)
                .mapValues(col -> candidateFisGenerator.getColumnsFilteredByMinSupport(col, minSuppCount))
                .sortByKey()
                .values()
                .collect();
    }

    public JavaRDD<Tuple2<int[], int[]>> toRddOfRanks1And2(
            JavaRDD<int[]> filteredTrs, CurrSizeFiRanks preprocessedF2) {
        return filteredTrs.map(tr -> candidateFisGenerator.toSortedRanks1And2(tr, preprocessedF2));
    }

    public JavaRDD<Tuple2<int[], int[]>> toRddOfRanks1AndK(
            JavaRDD<Tuple2<int[], int[]>> ranks1AndKm1, CurrSizeFiRanks preprocessedFk) {
        return ranks1AndKm1.map(row -> candidateFisGenerator.toSortedRanks1AndK(row._1, row._2, preprocessedFk));
    }

    public List<int[]> computeFk(
            int k, JavaRDD<Tuple2<int[], int[]>> ranks1AndKm1, NextSizeItemsetGenHelper genHelper) {
        return ranks1AndKm1
                .flatMap(tr -> new IteratorOverArray<>(
                        candidateFisGenerator.genNextSizeCands_ByItems(k-1, tr, genHelper)))
                .mapToPair(col -> new Tuple2<>(col[0], col))
                .foldByKey(new int[]{}, candidateFisGenerator::mergeColumns)
                .mapValues(col -> candidateFisGenerator.getColumnsFilteredByMinSupport(col, minSuppCount))
                .sortByKey()
                .values()
                .collect();
    }

    public JavaRDD<long[]> toRddOfTids(JavaRDD<Tuple2<int[], int[]>> ranks1AndK, TidsGenHelper tidsGenHelper) {
        return ranks1AndK
                .zipWithIndex()
                .flatMap(trAndTid -> new IteratorOverArray<>(
                        candidateFisGenerator.getRankToTid(trAndTid._1._2, trAndTid._2, tidsGenHelper)))
                .mapToPair(col -> new Tuple2<>(col[0], col))
                .foldByKey(new long[]{}, candidateFisGenerator::mergeTids)
                .sortByKey()
                .values();
    }

    public JavaRDD<long[]> toRddOfTidsNew(
            JavaRDD<Tuple2<int[], int[]>> ranks1AndK, TidsGenHelper tidsGenHelper, long totalTids) {
        return ranks1AndK
                .zipWithIndex()
                .flatMap(trAndTid -> new IteratorOverArray<>(
                        candidateFisGenerator.getRankToTid(trAndTid._1._2, trAndTid._2, tidsGenHelper)))
                .mapToPair(col -> new Tuple2<>(col[0], col))
                .aggregateByKey(
                        new long[]{},
                        (tidSet, elem) -> TidMergeSet.mergeElem(tidSet, elem, totalTids),
                        TidMergeSet::mergeSets
                )
                .sortByKey()
                .values();
    }
    public JavaRDD<long[]> toRddOfTidsNew2(
            JavaRDD<Tuple2<int[], int[]>> ranks1AndK, TidsGenHelper tidsGenHelper, long totalTids) {
        return ranks1AndK
                .zipWithIndex()
                .flatMap(trAndTid -> new IteratorOverTidAndRanksBitset(
                        candidateFisGenerator.getRankToTidNew(trAndTid._1._2, trAndTid._2, tidsGenHelper)))
                .mapToPair(rankAndTid -> new Tuple2<>(rankAndTid._1, rankAndTid))
                .aggregateByKey(
                        new long[]{},
                        (tidSet, elem) -> TidMergeSet.mergeElem(tidSet, elem, totalTids),
                        TidMergeSet::mergeSets
                )
                .sortByKey()
                .values();
    }
    public JavaRDD<long[]> toRddOfTidsNew3(
            JavaRDD<Tuple2<int[], int[]>> ranks1AndK, TidsGenHelper tidsGenHelper, long totalTids) {
        return ranks1AndK
                .zipWithIndex()
                .flatMap(trAndTid -> new IteratorOverTidAndRanksBitset(
                        candidateFisGenerator.getRankToTidNew(trAndTid._1._2, trAndTid._2, tidsGenHelper)))
                .mapToPair(rankAndTid -> new Tuple2<>(rankAndTid._1, rankAndTid))
                .aggregateByKey(
                        new long[]{},
                        (tidSet, elem) -> TidMergeSetNew.mergeElem(tidSet, elem, totalTids),
                        TidMergeSetNew::mergeSets
                )
                .sortByKey()
                .values();
    }
    public JavaRDD<long[]> toRddOfTidsNew4(JavaRDD<long[]> tidAndRanksBitset, long totalTids) {
        return tidAndRanksBitset
                .flatMap(IteratorOverTidAndRanksBitset::new)
                .mapToPair(rankAndTid -> new Tuple2<>(rankAndTid._1, rankAndTid))
                .aggregateByKey(
                        new long[]{},
                        (tidSet, elem) -> TidMergeSetNew.mergeElem(tidSet, elem, totalTids),
                        TidMergeSetNew::mergeSets
                )
                .sortByKey()
                .values();
    }
    public JavaRDD<long[]> prepareToTidsGen(
            JavaRDD<Tuple2<int[], int[]>> ranks1AndK, TidsGenHelper tidsGenHelper, long totalTids) {
        return ranks1AndK
                .zipWithIndex()
                .map(trAndTid -> candidateFisGenerator.getRankToTidNew(trAndTid._1._2, trAndTid._2, tidsGenHelper));
    }

    public JavaRDD<List<Long>> tmpToListOfTidLists(JavaRDD<long[]> tidsRdd) {
        return tidsRdd.map(this::tmpToShortedTids);
    }
    public List<Long> tmpToShortedTids(long[] tids) {
        if (tids.length == 0) {
            return Collections.emptyList();
        }
        int totElemsToCopy = Math.min(tids.length, 100);
        List<Long> res = new ArrayList<>(2 + totElemsToCopy);
        res.add(tids[0]);
        res.add((long)tids.length-1);
        for (int ii=1; ii<totElemsToCopy; ++ii) {
            res.add(tids[ii]);
        }
        return res;
    }
    public JavaRDD<List<Long>> tmpToListOfTidListsNew(JavaRDD<long[]> tidsRdd, int maxTidsInRes) {
        return tidsRdd.map(tidSet -> TidMergeSet.toNormalListOfTids(tidSet, maxTidsInRes));
    }

    public JavaRDD<List<Long>> tmpToTidCnts(JavaRDD<long[]> tidsRdd) {
        return tidsRdd.map(TidMergeSet::describeAsList);
    }

    public JavaRDD<List<Long>> tmpToTidCntsNew(JavaRDD<long[]> tidsRdd) {
        return tidsRdd.map(TidMergeSetNew::describeAsList);
    }

    public List<int[]> fkAsArraysToRankPairs(List<int[]> cols) {
        List<int[]> res = new ArrayList<>(cols.size() * cols.size());
        for (int[] col : cols) {
            res.addAll(candidateFisGenerator.fkColToPairs(col));
        }
        return res;
    }

    public List<FreqItemset<String>> fkAsArraysToResItemsets(
            List<int[]> cols, Map<String, Integer> itemToRank, CurrSizeFiRanks... fkToF2RanksArr) {
        String[] rankToItem = BasicOps.getRankToItem(itemToRank);

        List<FreqItemset<String>> res = new ArrayList<>((int) Math.pow(cols.size(), 3));
        for (int[] col : cols) {
            List<int[]> itemAndPairRanks = candidateFisGenerator.fkColToPairs(col);
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
