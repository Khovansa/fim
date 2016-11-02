package org.openu.fimcmp.apriori;

import org.openu.fimcmp.BasicOps;
import org.openu.fimcmp.FreqItemset;
import org.openu.fimcmp.util.IteratorOverArray;
import org.openu.fimcmp.util.ListComparator;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
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
    private final BasicOps basicOps;
    private final AprCandidateFisGenerator<T> candidateFisGenerator;
    private final ListComparator<T> listComparator;

    public AprioriAlg(long minSuppCount) {
        this.minSuppCount = minSuppCount;
        this.basicOps = new BasicOps();
        this.candidateFisGenerator = new AprCandidateFisGenerator<>();
        this.listComparator = new ListComparator<>();
    }

    public JavaPairRDD<T, Integer> computeF1(JavaRDD<? extends Collection<T>> trs) {
        return basicOps.countAndFilterByMinSupport(trs, minSuppCount);
    }

    /**
     * @return frequent items sorted by decreasing frequency
     */
    public List<T> computeF1New(JavaRDD<T[]> data) {
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

    public List<Integer[]> computeF2New(JavaRDD<Integer[]> filteredTrs) {
        return filteredTrs
                .flatMap(tr -> new IteratorOverArray<>(candidateFisGenerator.genTransactionC2sNew(tr)))
                .mapToPair(col -> new Tuple2<>(col[0], col))
                .foldByKey(new Integer[]{}, candidateFisGenerator::mergeC2Columns)
                .mapValues(col -> candidateFisGenerator.getC2sFilteredByMinSupport(col, minSuppCount))
                .sortByKey()
                .values()
                .collect();
    }

    public List<Integer[]> computeF3New(JavaRDD<Integer[]> filteredTrs, PreprocessedF2 preprocessedF2) {
        return filteredTrs
                .flatMap(tr -> new IteratorOverArray<>(
                        candidateFisGenerator.genTransactionC3sNew_ByItems(tr, preprocessedF2)))
                .mapToPair(col -> new Tuple2<>(col[0], col))
                .foldByKey(new Integer[]{}, candidateFisGenerator::mergeC2Columns)
                .mapValues(col -> candidateFisGenerator.getC2sFilteredByMinSupport(col, minSuppCount))
                .sortByKey()
                .values()
                .collect();
    }

    public JavaRDD<Tuple2<Integer[], Integer[]>> toRddOfRanks1And2(
            JavaRDD<Integer[]> filteredTrs, PreprocessedF2 preprocessedF2) {
        return filteredTrs.map(tr -> candidateFisGenerator.toSortedRanks1And2(tr, preprocessedF2));
    }

    public List<Integer[]> computeF3New2(
            JavaRDD<Tuple2<Integer[], Integer[]>> ranks1And2, PreprocessedF2 preprocessedF2) {
        return ranks1And2
                .flatMap(tr -> new IteratorOverArray<>(
                        candidateFisGenerator.genTransactionC3sNew2_ByItems(tr, preprocessedF2)))
                .mapToPair(col -> new Tuple2<>(col[0], col))
                .foldByKey(new Integer[]{}, candidateFisGenerator::mergeC2Columns)
                .mapValues(col -> candidateFisGenerator.getC2sFilteredByMinSupport(col, minSuppCount))
                .sortByKey()
                .values()
                .collect();
    }
    public List<Integer[]> computeF3New3(JavaRDD<Integer[]> filteredTrs, PreprocessedF2 preprocessedF2) {
        return filteredTrs
                .flatMap(tr -> new IteratorOverArray<>(
                        candidateFisGenerator.genTransactionC3sNew3_ByItems(tr, preprocessedF2)))
                .mapToPair(col -> new Tuple2<>(col[0], col))
                .foldByKey(new Integer[]{}, candidateFisGenerator::mergeC2Columns)
                .mapValues(col -> candidateFisGenerator.getC2sFilteredByMinSupport(col, minSuppCount))
                .sortByKey()
                .values()
                .collect();
    }

    public List<Integer[]> f2AsArraysToRankPairs(List<Integer[]> cols) {
        List<Integer[]> res = new ArrayList<>(cols.size() * cols.size());
        for (Integer[] col : cols) {
            res.addAll(candidateFisGenerator.f2ColToPairs(col));
        }
        return res;
    }

    public List<FreqItemset<String>> f2AsArraysToPairs(
            List<Integer[]> cols, Map<String, Integer> itemToRank) {
        String[] rankToItem = BasicOps.getRankToItem(itemToRank);
        List<FreqItemset<String>> res = new ArrayList<>(cols.size() * cols.size());
        for (Integer[] col : cols) {
            List<Integer[]> pairs = candidateFisGenerator.f2ColToPairs(col);
            for (Integer[] pair : pairs) {
                String elem1 = rankToItem[pair[0]];
                String elem2 = rankToItem[pair[1]];
                res.add(new FreqItemset<>(Arrays.asList(elem1, elem2), pair[2]));
            }
        }
        return res;
    }

    public List<FreqItemset<String>> f3AsArraysToTriplets(
            List<Integer[]> cols, Map<String, Integer> itemToRank, PreprocessedF2 preprocessedF2) {
        String[] rankToItem = BasicOps.getRankToItem(itemToRank);

        List<FreqItemset<String>> res = new ArrayList<>((int)Math.pow(cols.size(), 3));
        for (Integer[] col : cols) {
            List<Integer[]> itemAndPairRanks = candidateFisGenerator.f2ColToPairs(col);
            for (Integer[] itemAndPairRank : itemAndPairRanks) {
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

    public JavaRDD<Collection<List<T>>> computeCand2(JavaRDD<ArrayList<T>> trs) {
        return trs.map(candidateFisGenerator::genTransactionC2s);
    }

    public JavaPairRDD<List<T>, Integer> countAndFilterByMinSupport(JavaRDD<Collection<List<T>>> trsAsCandidatesList) {
        return basicOps.countAndFilterByMinSupport(trsAsCandidatesList, minSuppCount);
    }

    public JavaRDD<Collection<List<T>>> computeNextSizeCandidates(
            JavaRDD<Collection<List<T>>> trsAsCandidatesOfSizeK, int k, Set<T> f1, Set<List<T>> oldFisOfSizeK) {
        return trsAsCandidatesOfSizeK.map(
                    tr -> candidateFisGenerator.getNextSizeCandItemsetsFromTransaction(tr, k, f1, oldFisOfSizeK));
    }

    public Collection<List<T>> toCollectionOfLists(JavaPairRDD<List<T>, Integer> rdd) {
//        List<List<T>> res = new ArrayList<>(10000);
//        basicOps.fillCollectionFromRdd(res, rdd);
//        return res;
        return rdd.map(Tuple2::_1).collect();
    }

    public Set<T> getUpdatedF1(Set<T> f1, Collection<? extends Collection<T>> kFIs) {
        Set<T> res = new HashSet<>(f1.size());
        for (Collection<T> fi : kFIs) {
            for (T item : fi) {
                if (f1.contains(item)) {
                    res.add(item);
                }
            }
        }
        return res;
    }

    public ListComparator<T> getListComparator() {
        return listComparator;
    }
}
