package org.openu.fimcmp.apriori;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import org.junit.Before;
import org.junit.Test;
import org.openu.fimcmp.AlgITBase;
import org.openu.fimcmp.BasicOps;
import org.openu.fimcmp.FreqItemset;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AprioriAlgIT extends AlgITBase {
    @SuppressWarnings("FieldCanBeLocal")
    private AprioriAlg<String> apr;

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void test() throws Exception {
        try {
            runIt();
        } finally {
            Thread.sleep(100_000_000);
        }
    }

    private void runIt() throws Exception {
        final PrepStepOutputAsArr prep = prepareAsArr("pumsb.dat", 0.4, false);
//        final PrepStepOutputAsArr prep = prepareAsArr("my.small.txt", 0.1, false);
        apr = new AprioriAlg<>(prep.minSuppCount);
        List<String> sortedF1 = apr.computeF1(prep.trs);
        final int totalFreqItems = sortedF1.size();
        pp("F1 size = " + totalFreqItems);
        pp(sortedF1);
        Map<String, Integer> itemToRank = BasicOps.itemToRank(sortedF1);
        //from now on, the items are [0, sortedF1.size), 0 denotes the most frequent item

        //TODO: keep the partition of the transaction!
        JavaRDD<int[]> filteredTrs = prep.trs.map(t -> BasicOps.getMappedFilteredAndSortedTrs(t, itemToRank));
        filteredTrs = filteredTrs.persist(StorageLevel.MEMORY_ONLY_SER());
//        filteredTrs = filteredTrs.persist(StorageLevel.MEMORY_AND_DISK_SER());
        pp("filtered and saved");

        List<int[]> f2AsArrays = apr.computeF2(filteredTrs, totalFreqItems);
        pp("F2 as arrays size: "+f2AsArrays.size());
        List<int[]> f2 = apr.fkAsArraysToRankPairs(f2AsArrays, prep.minSuppCount);
        pp("F2 size: "+f2.size());
        List<FreqItemset<String>> f2Res = apr.fkAsArraysToResItemsets(prep.minSuppCount, f2AsArrays, itemToRank);
        f2Res = f2Res.stream().sorted((fi1, fi2) -> Integer.compare(fi2.freq, fi1.freq)).collect(Collectors.toList());
        pp("F2: "+StringUtils.join(f2Res.subList(0, Math.min(3, f2Res.size())), "\n"));

        PairRanks f2Ranks = CurrSizeFiRanks.constructF2Ranks(f2, totalFreqItems);
        CurrSizeFiRanks preprocessedF2 = CurrSizeFiRanks.construct(f2, totalFreqItems, totalFreqItems, f2Ranks);
        JavaRDD<Tuple2<int[], long[]>> ranks1And2 = apr.toRddOfRanks1And2(filteredTrs, preprocessedF2);
        ranks1And2 = ranks1And2.persist(StorageLevel.MEMORY_ONLY_SER());
//        ranks1And2 = ranks1And2.persist(StorageLevel.MEMORY_AND_DISK_SER());
        pp("zzz");
//        List<int[]> f3AsArrays = apr.computeFk(3, ranks1And2, preprocessedF2);
        List<int[]> f3AsArrays = apr.computeFk_Part(3, ranks1And2, preprocessedF2);
        pp("F3 as arrays size: "+f3AsArrays.size());
        List<int[]> f3 = apr.fkAsArraysToRankPairs(f3AsArrays, prep.minSuppCount);
        pp("F3 size: "+f3.size());
        List<FreqItemset<String>> f3Res = apr.fkAsArraysToResItemsets(
                prep.minSuppCount, f3AsArrays, itemToRank, preprocessedF2);
        f3Res = f3Res.stream().sorted((fi1, fi2) -> Integer.compare(fi2.freq, fi1.freq)).collect(Collectors.toList());
        pp("F3: " + StringUtils.join(f3Res.subList(0, Math.min(10, f3Res.size())), "\n"));

        CurrSizeFiRanks preprocessedF3 = CurrSizeFiRanks.construct(f3, totalFreqItems, f2.size(), f2Ranks);
        JavaRDD<Tuple2<int[], long[]>> ranks1And3 = apr.toRddOfRanks1AndK(ranks1And2, preprocessedF3);
//        ranks1And3 = ranks1And3.persist(StorageLevel.MEMORY_AND_DISK_SER());

//        List<Integer> lens = ranks1And3.map(t -> t._2.length).collect();
//        List<Integer> lens = ranks1And3.map(t -> BitArrays.cardinality(t._2, 0)).collect();
//        long sum = 0;
//        for (Integer len : lens) {
//            sum += len;
//        }
//        pp("Avg 3-ranks count: " + 1.0 * sum / lens.size());

        TidsGenHelper tidsGenHelper = preprocessedF3.constructTidGenHelper(f3, (int)prep.totalTrs);
        JavaRDD<long[]> tidAndRanksBitset = apr.prepareToTidsGen(ranks1And3, tidsGenHelper);
//        tidAndRanksBitset = tidAndRanksBitset.persist(StorageLevel.MEMORY_ONLY_SER());
        tidAndRanksBitset = tidAndRanksBitset.persist(StorageLevel.MEMORY_AND_DISK_SER());

        pp("Starting collecting the TIDs");
        long[][] rankKToTids = apr.computeCurrRankToTidBitSet(tidAndRanksBitset, prep.totalTrs, tidsGenHelper);

        pp("TIDs:");
            for (int rankK = 0, cnt=0; rankK < 500 && cnt<20; ++rankK) {
                if (rankKToTids[rankK] != null) {
                    ++cnt;
                    System.out.println(Arrays.toString(TidMergeSet.describeAsList(TidMergeSet.withMetadata(rankKToTids[rankK]))));
                }
            }

//        pp("zzz");
//        List<int[]> f4AsArrays = apr.computeFk(4, ranks1And3, preprocessedF3);
//        pp("F4 as arrays size: "+f4AsArrays.size());
//        List<int[]> f4 = apr.fkAsArraysToRankPairs(f4AsArrays);
//        pp("F4 size: "+f4.size());
//        List<FreqItemset<String>> f4Res = apr.fkAsArraysToResItemsets(f4AsArrays, itemToRank, preprocessedF3, preprocessedF2);
//        f4Res = f4Res.stream().sorted((fi1, fi2) -> Integer.compare(fi2.freq, fi1.freq)).collect(Collectors.toList());
//        pp("F4: " + StringUtils.join(f4Res.subList(0, Math.min(3, f4Res.size())), "\n"));
    }
}
