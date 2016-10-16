package my.apriori;

import my.BasicOps;
import my.IteratorOverArray;
import my.ListComparator;
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
