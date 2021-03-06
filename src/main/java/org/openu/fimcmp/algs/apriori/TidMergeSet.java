package org.openu.fimcmp.algs.apriori;

import org.openu.fimcmp.itemset.ItemsetAndTids;
import org.openu.fimcmp.itemset.ItemsetAndTidsCollection;
import org.openu.fimcmp.util.Assert;
import org.openu.fimcmp.util.BitArrays;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.*;

/**
 * Set of TIDs optimized for merge operations. <br/>
 * Holds everything in long[]. <br/>
 * To allow working with RDD&lt;long[]> rather than with RDD&lt;TidMergeSet>,
 * has all its operations as static, taking long[] as its first argument. <br/>
 * <b>WARNING: assumes all the TIDs are in the range [0, total), see RDD.zipWithIndex()</b><br/>
 * <p>
 * <pre>
 * The array structure is: {rank, size, firstElem, lastElem, (bitset of TIDs)}.
 * Bitset of TIDs:
 * - The size is 2 x total / 64
 * - Each even element holds a bitset for the 64 TIDs in the range [ii, ii+63], where ii = elem index / 2
 * - Each odd element holds a pointer to the next element
 * This way we
 * (1) Avoid re-allocations
 * (2) merge(s1, s2) only takes O(s2.size / 64) operations,
 *     e.g. if it holds only 2 elements it will take about 2 operations
 * </pre>
 */
public class TidMergeSet implements Serializable {
    private static final int RANK_IND = 0;
    private static final int SIZE_IND = 1;
    private static final int MIN_ELEM_IND = 2;
    private static final int MAX_ELEM_IND = 3;
    private static final int MIN_TID_IN_PARTITION_IND = 4;
    private static final int MAX_TID_IN_PARTITION_IND = 5;
    private static final int BITSET_START_IND = 6;

    /**
     * Compute a mapping rankK -> 'tid-set'. <br/>
     * The 'tid-set' is a bitset of TIDs prefixed with some metadata. <br/>
     * @param kRanksBsAndTidIt iterator over transactions in this partition. <br/>
     *                         Each transaction holds a bitset of its k-FI ranks and a transaction ID
     * @param minAndMaxTids    Min and max TID in this partition
     * @return iterator over a single long[][] array that holds an entire mapping rankK to 'tid-set' for this partition
     */
    static Iterator<long[][]> processPartition(
            Iterator<Tuple2<long[], Long>> kRanksBsAndTidIt,
            TidsGenHelper tidsGenHelper,
            Tuple2<Long, Long> minAndMaxTids) {
        final int RANKS_BITSET_START_IND = 0;
        long[][] rankKToTidSet = new long[tidsGenHelper.totalRanks()][];
        boolean hasElems = false;

        final long minTidInPartition = minAndMaxTids._1;
        final int tidCorrection = getTidCorrection(minTidInPartition);
        final long maxTidInPartition = minAndMaxTids._2;
        final int totalTidsInThisPartition = 1 + (int)(maxTidInPartition - tidCorrection);
        while(kRanksBsAndTidIt.hasNext()) {
            hasElems = true;
            Tuple2<long[], Long> kRanksBsAndTid = kRanksBsAndTidIt.next();
            long[] kRanksBs = kRanksBsAndTid._1;

            long[] kRanksToBeStoredBs = new long[kRanksBs.length];
            tidsGenHelper.setToResRanksToBeStoredBitSet(kRanksToBeStoredBs, RANKS_BITSET_START_IND, kRanksBs);
            int[] kRanksToBeStored = BitArrays.asNumbers(kRanksToBeStoredBs, RANKS_BITSET_START_IND);

            //storing a smaller number than TID:
            final long tidToStore = kRanksBsAndTid._2 - tidCorrection;
            for (int rankK : kRanksToBeStored) {
                long[] tidSet = rankKToTidSet[rankK];
                if (tidSet != null) {
                    BitArrays.set(tidSet, BITSET_START_IND, (int) tidToStore); //requires to set min, max and size later
                } else {
                    rankKToTidSet[rankK] = newSetWithElem(rankK, tidToStore, totalTidsInThisPartition);
                    rankKToTidSet[rankK][MIN_TID_IN_PARTITION_IND] = minTidInPartition;
                    rankKToTidSet[rankK][MAX_TID_IN_PARTITION_IND] = maxTidInPartition;
                }
            }
        }

        if (hasElems) {
            for (long[] tidSet : rankKToTidSet) {
                setMetadata(tidSet);
            }
        }

        return Collections.singletonList(rankKToTidSet).iterator();
    }

    /**
     * @return iterator over a single triple (partitionIndex, min-TID in this partition, max-TID in this partition)
     */
    static Iterator<Tuple3<Integer, Long, Long>> findMinAndMaxTids(
            Integer partitionIndex,
            Iterator<Tuple2<long[], Long>> kRanksBsAndTidIt) {
        long minTid = Long.MAX_VALUE;
        long maxTid = Long.MIN_VALUE;
        while (kRanksBsAndTidIt.hasNext()) {
            Tuple2<long[], Long> kRanksBsAndTid = kRanksBsAndTidIt.next();
            long tid = kRanksBsAndTid._2;
            minTid = Math.min(minTid, tid);
            maxTid = Math.max(maxTid, tid);
        }
        return Collections.singletonList(new Tuple3<>(partitionIndex, minTid, maxTid)).iterator();
    }

    static long[][] mergePartitions(long[][] part1, long[][] part2, TidsGenHelper tidsGenHelper) {
        if (part2.length == 0) {
            return part1;
        }
        final int totalRanks = tidsGenHelper.totalRanks();
        if (part1.length == 0) {
            part1 = new long[totalRanks][];
        }

        for (int rankK = 0; rankK < totalRanks; ++rankK) {
            part1[rankK] = mergeTidSets(part1[rankK], part2[rankK]);
        }
        return part1;
    }

    /**
     * @param tidSets 'short' TID sets generated by separate partitions. <br/>
     *                There might be many TID sets with the same rankK. <br/>
     *                'Short' means that they cover only a range of TIDs.
     * @return for each rankK, a TID set that covers all TIDs, in a format {rankK, TID-set as a bit array}
     */
    public static ItemsetAndTidsCollection mergeTidSetsWithSameRankDropMetadata(
            List<long[]> tidSets, TidsGenHelper tidsGenHelper, FiRanksToFromItems fiRanksToFromItems) {
        final int RES_BITSET_START_IND = 0;

        final Map<Integer, List<long[]>> kRankToTidSets = computeRankToTidSets(tidSets);
        final int totalTids = tidsGenHelper.totalTids();
        ArrayList<ItemsetAndTids> resList = new ArrayList<>(kRankToTidSets.size());
        for (Map.Entry<Integer, List<long[]>> entry : kRankToTidSets.entrySet()) {
            List<long[]> tidSetsList = entry.getValue();
            long[] resTidSet = new long[BitArrays.requiredSize(totalTids, RES_BITSET_START_IND)];
            mergeShortSetsToLongWithoutMetadata(tidSetsList, resTidSet, RES_BITSET_START_IND);
            int rankK = entry.getKey();
            if (!tidsGenHelper.isStoreContainingTid(rankK)) {
                BitArrays.not(resTidSet, RES_BITSET_START_IND, totalTids - 1);
            }

            int supportCnt = tidsGenHelper.getSupportCount(rankK);
//            Assert.isTrue(supportCnt == BitArrays.cardinality(resTidSet, RES_BITSET_START_IND));
            int[] itemset = fiRanksToFromItems.getItemsetByRankMaxK(rankK);
            ItemsetAndTids res = new ItemsetAndTids(itemset, resTidSet, supportCnt);
            resList.add(res);
        }

        int itemsetSize = fiRanksToFromItems.getMaxK();
        return new ItemsetAndTidsCollection(resList, itemsetSize, totalTids);
    }

    private static Map<Integer, List<long[]>> computeRankToTidSets(List<long[]> tidSets) {
        Map<Integer, List<long[]>> kRankToTidSets = new TreeMap<>();
        for (long[] tidSet : tidSets) {
            int rankK = (int)tidSet[RANK_IND];
            putToMapOfLists(kRankToTidSets, rankK, tidSet, 100);
        }
        return kRankToTidSets;
    }

    /**
     * Assumption: the short sets come from different partitions, so their TID sets are not just mutually exclusive,
     * but also form mutually exclusive ranges. <br/>
     * <p/>
     * E.g. the first sets can only have TIDs from 0 to 67 and the second one can only have TIDs from 68 to 132. <br/>
     * Yet to keep the TID sets short, the TIDs are stored with their 'base' part subtracted. <br/>
     * TIDs bit set in the range [0, 67] will have the correction of 0. <br/>
     * But TIDs bit set [68, 133] will have the correction of 64 and the bit set will store the numbers in the range
     * [4, 69]. <br/>
     */
    private static void mergeShortSetsToLongWithoutMetadata(
            List<long[]> tidSetsList, long[] resTidSet, int resBitsetStartInd) {
        for (long[] shortTidSet : tidSetsList) {
            if (shortTidSet[SIZE_IND] > 0) {
                int copyLen = shortTidSet.length - BITSET_START_IND;

                int tidCorrection = getTidCorrection(shortTidSet[MIN_TID_IN_PARTITION_IND]);
                //that's the whole trick: we correct the TIDs by simply shifting them to the right position
                int resStartInd = BitArrays.wordIndex(tidCorrection, resBitsetStartInd);

                addTidSetProducedByPartition(resTidSet, resStartInd, shortTidSet, copyLen);
            }
        }
    }

    /**
     * Assuming the 'shortTidSet' contains its own exclusive range of TIDs.
     */
    private static void addTidSetProducedByPartition(
            long[] resTidSet, int resStartInd, long[] shortTidSet, int copyLen) {
        //the short sets' bitset might still intersect at its ends with others, since one 'long' holds up to 63 TIDs
        //=> take care of the ends:
        long firstResElem = resTidSet[resStartInd];
        final int lastElemInd = resStartInd + copyLen - 1;
        long lastResElem = resTidSet[lastElemInd];

        System.arraycopy(shortTidSet, BITSET_START_IND, resTidSet, resStartInd, copyLen);

        resTidSet[resStartInd] |= firstResElem;
        resTidSet[lastElemInd] |= lastResElem;
    }

    /**
     * Assuming the two sets' ranges do not intersect
     */
    private static long[] mergeTidSets(long[] s1, long[] s2) {
        if (s2 == null || s2.length <= 1) {
            return s1;
        }
        if (s1 == null || s1.length <= 1) {
            return copyOf(s2);
        }

        final boolean s1IsLower = s1[MIN_ELEM_IND] < s2[MIN_ELEM_IND];
        long[] lowerSet = s1IsLower ? s1 : s2;
        long[] higherSet = s1IsLower ? s2 : s1;
        Assert.isTrue(lowerSet[MAX_ELEM_IND] < higherSet[MIN_ELEM_IND]);
        Assert.isTrue(lowerSet[MAX_TID_IN_PARTITION_IND] < higherSet[MIN_TID_IN_PARTITION_IND]);

        int maxMinusCorrection = (int) (higherSet[MAX_TID_IN_PARTITION_IND] - lowerSet[MIN_TID_IN_PARTITION_IND]);
        long[] res = new long[BitArrays.requiredSize(1 + maxMinusCorrection, BITSET_START_IND)];
        res[RANK_IND] = lowerSet[RANK_IND];
        res[SIZE_IND] = lowerSet[SIZE_IND] + higherSet[SIZE_IND];
        res[MIN_ELEM_IND] = lowerSet[MIN_ELEM_IND];
        res[MAX_ELEM_IND] = higherSet[MAX_ELEM_IND];
        res[MIN_TID_IN_PARTITION_IND] = lowerSet[MIN_TID_IN_PARTITION_IND];
        res[MAX_TID_IN_PARTITION_IND] = higherSet[MAX_TID_IN_PARTITION_IND];

        int lowerSetCopyLen = lowerSet.length - BITSET_START_IND;
        System.arraycopy(lowerSet, BITSET_START_IND, res, BITSET_START_IND, lowerSetCopyLen);

        int higherSetCopyLen = higherSet.length - BITSET_START_IND;
        int tidCorrection = getTidCorrection(higherSet[MIN_TID_IN_PARTITION_IND]);
        int higherSetDestStartInd = BitArrays.wordIndex(tidCorrection, BITSET_START_IND);
        addTidSetProducedByPartition(res, higherSetDestStartInd, higherSet, higherSetCopyLen);

        return res;
    }

    private static void setMetadata(long[] tidSet) {
        if (tidSet == null) {
            return;
        }

        int tidCorrection = getTidCorrection(tidSet[MIN_TID_IN_PARTITION_IND]);
        tidSet[MIN_ELEM_IND] = tidCorrection + BitArrays.min(tidSet, BITSET_START_IND);
        tidSet[MAX_ELEM_IND] = tidCorrection + BitArrays.max(tidSet, BITSET_START_IND);
        tidSet[SIZE_IND] = BitArrays.cardinality(tidSet, BITSET_START_IND);
    }

    public static long[] describeAsList(long[] tidSet) {
        long[] res = new long[4];
        res[0] = tidSet[RANK_IND];
        if (tidSet.length > 1) {
            res[1] = tidSet[SIZE_IND];
            res[2] = tidSet[MIN_ELEM_IND];
            res[3] = tidSet[MAX_ELEM_IND];
//            res.add((long)count(tidSet));
        }

        return res;
    }


    private static long[] newSetWithElem(int rank, long tid, long totalTids) {
        long[] res = new long[BitArrays.requiredSize((int) totalTids, BITSET_START_IND)];
        res[RANK_IND] = rank;
        res[SIZE_IND] = 1;
        int tidAsInt = (int) tid;
        res[MIN_ELEM_IND] = res[MAX_ELEM_IND] = tidAsInt;
        BitArrays.set(res, BITSET_START_IND, tidAsInt);
        return res;
    }

    static long[] newEmptySet(int rank) {
        long[] res = new long[BITSET_START_IND + 1];
        res[RANK_IND] = rank;
        res[SIZE_IND] = 0;
        res[MIN_ELEM_IND] = res[MAX_ELEM_IND] = -1;
        return res;
    }

    private static int getTidCorrection(long minTidInPartition) {
        return BitArrays.roundedToWordStart((int)minTidInPartition);
    }

    private static long[] copyOf(long[] s2) {
        return Arrays.copyOf(s2, s2.length);
    }

    private static <K, V> void putToMapOfLists(Map<K, List<V>> keyToValList, K key, V val, int valsInitCapacity) {
        List<V> tidSetsList = keyToValList.get(key);
        if (tidSetsList == null) {
            tidSetsList = new ArrayList<>(valsInitCapacity);
            keyToValList.put(key, tidSetsList);
        }
        tidSetsList.add(val);
    }
}
