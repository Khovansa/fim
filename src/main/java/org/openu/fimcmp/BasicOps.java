package org.openu.fimcmp;

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

    public JavaRDD<ArrayList<String>> readLinesAsSortedItemsList(String inputFile, int numPart, JavaSparkContext sc) {
        JavaRDD<String> lines = sc.textFile(inputFile, numPart);
        return linesAsSortedItemsList(lines);
    }

    public static JavaRDD<String[]> readLinesAsSortedItemsArr(String inputFile, int numPart, JavaSparkContext sc) {
        JavaRDD<String> lines = sc.textFile(inputFile, numPart);
        return linesAsSortedItemsArr(lines);
    }

    public JavaRDD<ArrayList<String>> linesAsSortedItemsList(JavaRDD<String> lines) {
        return lines.map(BasicOps::splitLineToSortedList);
    }

    public static JavaRDD<String[]> linesAsSortedItemsArr(JavaRDD<String> lines) {
        return lines.map(BasicOps::splitLineToSortedArr);
    }

    public static <T> Map<T, Integer> itemToRank(List<T> f1) {
        Map<T, Integer> res = new HashMap<>(f1.size() * 2);
        int rank=0;
        for (T item : f1) {
            res.put(item, rank);
            ++rank;
        }
        return res;
    }

    public static <T> int[] getMappedFilteredAndSortedTrs(T[] tr, Map<T, Integer> itemToRank) {
        int resCnt = 0;
        for (T item : tr) {
            if (itemToRank.get(item) != null) {
                ++resCnt;
            }
        }

        int[] res = new int[resCnt];
        int ii=0;
        for (T item : tr) {
            Integer rank = itemToRank.get(item);
            if (rank != null) {
                res[ii++] = rank;
            }
        }

        Arrays.sort(res); //smaller rank means more frequent
        return res;
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

    public static String[] getRankToItem(Map<String, Integer> itemToRank) {
        String[] rankToItem = new String[itemToRank.size()];
        for (Map.Entry<String, Integer> entry : itemToRank.entrySet()) {
            rankToItem[entry.getValue()] = entry.getKey();
        }
        return rankToItem;
    }

    static ArrayList<String> splitLineToSortedList(String line) {
        SortedSet<String> res = splitLineToSortedSet(line);
        return new ArrayList<>(res);
    }

    static String[] splitLineToSortedArr(String line) {
        SortedSet<String> res = splitLineToSortedSet(line);

        String[] resArr = new String[res.size()];
        int ii=0;
        for (String item : res) {
            resArr[ii++] = item;
        }
        return resArr;
    }

    private static SortedSet<String> splitLineToSortedSet(String line) {
        String[] items = line.split(" ");
        SortedSet<String> res = new TreeSet<>();
        for (String item : items) {
            res.add(item.trim());
        }
        return res;
    }
}
