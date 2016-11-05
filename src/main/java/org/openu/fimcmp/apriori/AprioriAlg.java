package org.openu.fimcmp.apriori;

import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaRDD;
import org.openu.fimcmp.BasicOps;
import org.openu.fimcmp.FreqItemset;
import org.openu.fimcmp.util.IteratorOverArray;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
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
                .foldByKey(new int[]{}, candidateFisGenerator::mergeC2Columns)
                .mapValues(col -> candidateFisGenerator.getC2sFilteredByMinSupport(col, minSuppCount))
                .sortByKey()
                .values()
                .collect();
    }

    public JavaRDD<Tuple2<int[], int[]>> toRddOfRanks1And2(
            JavaRDD<int[]> filteredTrs, PreprocessedF2 preprocessedF2) {
        return filteredTrs.map(tr -> candidateFisGenerator.toSortedRanks1And2(tr, preprocessedF2));
    }

    public List<int[]> computeF3(
            JavaRDD<Tuple2<int[], int[]>> ranks1And2, NextSizeItemsetGenHelper genHelper) {
        return ranks1And2
                .flatMap(tr -> new IteratorOverArray<>(
                        candidateFisGenerator.genNextSizeCands_ByItems(2, tr, genHelper)))
                .mapToPair(col -> new Tuple2<>(col[0], col))
                .foldByKey(new int[]{}, candidateFisGenerator::mergeC2Columns)
                .mapValues(col -> candidateFisGenerator.getC2sFilteredByMinSupport(col, minSuppCount))
                .sortByKey()
                .values()
                .collect();
    }

    public List<int[]> f2AsArraysToRankPairs(List<int[]> cols) {
        List<int[]> res = new ArrayList<>(cols.size() * cols.size());
        for (int[] col : cols) {
            res.addAll(candidateFisGenerator.f2ColToPairs(col));
        }
        return res;
    }

    public List<FreqItemset<String>> f2AsArraysToPairs(
            List<int[]> cols, Map<String, Integer> itemToRank) {
        String[] rankToItem = BasicOps.getRankToItem(itemToRank);
        List<FreqItemset<String>> res = new ArrayList<>(cols.size() * cols.size());
        for (int[] col : cols) {
            List<int[]> pairs = candidateFisGenerator.f2ColToPairs(col);
            for (int[] pair : pairs) {
                String elem1 = rankToItem[pair[0]];
                String elem2 = rankToItem[pair[1]];
                res.add(new FreqItemset<>(Arrays.asList(elem1, elem2), pair[2]));
            }
        }
        return res;
    }

    public List<FreqItemset<String>> f3AsArraysToTriplets(
            List<int[]> cols, Map<String, Integer> itemToRank, PreprocessedF2 preprocessedF2) {
        String[] rankToItem = BasicOps.getRankToItem(itemToRank);

        List<FreqItemset<String>> res = new ArrayList<>((int) Math.pow(cols.size(), 3));
        for (int[] col : cols) {
            List<int[]> itemAndPairRanks = candidateFisGenerator.f2ColToPairs(col);
            for (int[] itemAndPairRank : itemAndPairRanks) {
                String elem1 = rankToItem[itemAndPairRank[0]];
                int[] pair = preprocessedF2.getPairByRank(itemAndPairRank[1]);
                String elem2 = rankToItem[pair[0]];
                String elem3 = rankToItem[pair[1]];
                int freq = itemAndPairRank[2];
                res.add(new FreqItemset<>(Arrays.asList(elem1, elem2, elem3), freq));
            }
        }

        return res;
    }
}
