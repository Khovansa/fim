package my;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * Provide basic operations/algorithms useful for all higher-level algorithms
 */
@SuppressWarnings("WeakerAccess")
public class BasicOps implements Serializable {

    public BasicOps() {
    }

    public JavaRDD<ArrayList<String>> readLinesAsSortedItems(String inputFile, JavaSparkContext sc) {
        JavaRDD<String> lines = sc.textFile(inputFile);
        return linesAsSortedItems(lines);//.persist(StorageLevel.MEMORY_ONLY());
    }

    public JavaRDD<ArrayList<String>> linesAsSortedItems(JavaRDD<String> lines) {
        return lines.map(BasicOps::splitLineToSortedList);
    }

    public static <V> ArrayList<V> withoutInfrequent(Collection<V> tr, Set<V> frequent) {
        if (tr.isEmpty() || frequent.isEmpty()) {
            return new ArrayList<>(0);
        }

        ArrayList<V> res = new ArrayList<>(tr.size());
        for (V item : tr) {
            if (frequent.contains(item)) {
                res.add(item);
            }
        }
        res.trimToSize();
        return res;
    }

    @SuppressWarnings("unused")
    public <T> JavaRDD<Tuple2<T, Integer>> computeSortedF1(JavaRDD<Set<T>> trs, double minSupp, boolean isAsc) {
        final long totalTrs = trs.count();
        final long minSuppCount = minSuppCount(totalTrs, minSupp);

        JavaPairRDD<T, Integer> f1 = countAndFilterByMinSupport(trs, minSuppCount);

        return sortedByFrequency(f1, isAsc);
    }

    public static long minSuppCount(long totalTrs, double minSupp) {
        return (long)Math.ceil(totalTrs * minSupp);
    }

    public <T> JavaPairRDD<T, Integer> countAndFilterByMinSupport(JavaRDD<? extends Collection<T>> trs, long minSuppCount) {
        JavaRDD<T> items = trs.flatMap(Collection::iterator);

        return items.
                mapToPair(x -> new Tuple2<>(x, 1)).
                reduceByKey((x, y) -> x + y).
                filter(t -> t._2() >= minSuppCount);
    }

    public <V> JavaRDD<Tuple2<V, Integer>> sortedByFrequency(JavaPairRDD<V, Integer> fis, boolean isAsc) {
        return fis.sortByKey(true).map(t -> new Tuple2<>(t._1(), t._2())).sortBy(Tuple2::_2, isAsc, 1);
    }

    public <V> JavaRDD<Tuple2<V, Integer>> sortedByFrequency(
            JavaPairRDD<V, Integer> fis, Comparator<V> comparator, boolean isAsc) {
        return fis.sortByKey(comparator, true).map(t -> new Tuple2<>(t._1, t._2())).sortBy(Tuple2::_2, isAsc, 1);
    }

    public <T> List<Tuple2<T, Integer>> fiRddAsList(JavaRDD<Tuple2<T, Integer>> fiRdd) {
        List<Tuple2<T, Integer>> res = new ArrayList<>();
        fiRdd.toLocalIterator().forEachRemaining(res::add);
        return res;
    }

    public <V, C extends Collection<V>> C fillCollectionFromRdd(C res, JavaPairRDD<V, Integer> rdd) {
        rdd.map(Tuple2::_1).toLocalIterator().forEachRemaining(res::add);
        return res;
    }

    static ArrayList<String> splitLineToSortedList(String line) {
        String[] items = line.split(" ");
        SortedSet<String> res = new TreeSet<>();
        for (String item : items) {
            res.add(item.trim());
        }
        return new ArrayList<>(res);
    }
}
