package org.openu.fimcmp.apriori;

import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;
import org.openu.fimcmp.BasicOps;
import org.openu.fimcmp.FreqItemset;
import org.openu.fimcmp.util.IteratorOverArray;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
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

    //TODO: repartition the TID sets based on rankK's
    //Copying FPGrowth:
    //after mapPartition():
    // rdd.flatMapToPair(rankToTidSet -> Iterator<Tuple2(partNumber, tidSet)>)
    // - the iterator is similar to IteratorOverArray,
    //   but at the end maps currRank to new Tuple2(p.part(currRank), arr[currRank])
    // The partitioner should map rankK to partition number based on the common prefix,
    // i.e. need to pre-compute a map: rankK -> prefixRankKm1
    //then aggregateByKey(new long[0][], TidMergeSet.mergePartitions, partitioner.numPartitions())
    //It will return a PairRdd<key, val>.
    //Eclat should be applied to the 'val' - completely independently
    //Then can just collect the results
    public long[][] computeCurrRankToTidBitSet_Part(
            JavaRDD<long[]> kRanksBsRdd, long totalTids, TidsGenHelper tidsGenHelper) {
        return kRanksBsRdd
                .zipWithIndex()
                .mapPartitions(kRanksBsAndTidIt -> TidMergeSet.processPartition(kRanksBsAndTidIt, tidsGenHelper, totalTids))
                .fold(new long[0][], (p1, p2) -> TidMergeSet.mergePartitions(p1, p2, tidsGenHelper));
    }

    public List<int[]> fkAsArraysToRankPairs(List<int[]> cols, long minSuppCount) {
        List<int[]> res = new ArrayList<>(cols.size() * cols.size());
        for (int[] col : cols) {
            res.addAll(candidateFisGenerator.fkColToPairs(col, minSuppCount));
        }
        return res;
    }

    public List<FreqItemset<String>> fkAsArraysToResItemsets(
            long minSuppCount, List<int[]> cols, Map<String, Integer> itemToRank, CurrSizeFiRanks... fkToF2RanksArr) {
        String[] rankToItem = BasicOps.getRankToItem(itemToRank);

        List<FreqItemset<String>> res = new ArrayList<>((int) Math.pow(cols.size(), 3));
        for (int[] col : cols) {
            List<int[]> itemAndPairRanks = candidateFisGenerator.fkColToPairs(col, minSuppCount);
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
