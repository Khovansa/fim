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
        final PrepStepOutputAsArr prep = prepareAsArr("pumsb.dat", 0.2, false);
//        final PrepStepOutputAsArr prep = prepareAsArr("my.small.txt", 0.1, false);
        apr = new AprioriAlg<>(prep.minSuppCount);
        List<String> sortedF1 = apr.computeF1(prep.trs);
        int totalFreqItems = sortedF1.size();
        pp("F1 size = " + totalFreqItems);
        pp(sortedF1);
        Map<String, Integer> itemToRank = BasicOps.itemToRank(sortedF1);
        //from now on, the items are [0, sortedF1.size), 0 denotes the most frequent item

        //TODO: keep the partition of the transaction!
        JavaRDD<int[]> filteredTrs = prep.trs.map(t -> BasicOps.getMappedFilteredAndSortedTrs(t, itemToRank));
        filteredTrs = filteredTrs.persist(StorageLevel.MEMORY_ONLY_SER());
        pp("filtered and saved");

        List<int[]> f2AsArrays = apr.computeF2(filteredTrs);
        pp("F2 as arrays size: "+f2AsArrays.size());
        List<int[]> f2 = apr.fkAsArraysToRankPairs(f2AsArrays);
        pp("F2 size: "+f2.size());
        List<FreqItemset<String>> f2Res = apr.fkAsArraysToResItemsets(f2AsArrays, itemToRank);
        f2Res = f2Res.stream().sorted((fi1, fi2) -> Integer.compare(fi2.freq, fi1.freq)).collect(Collectors.toList());
        pp("F2: "+StringUtils.join(f2Res.subList(0, Math.min(3, f2Res.size())), "\n"));

        PairRanks f2Ranks = CurrSizeFiRanks.constructF2Ranks(f2, totalFreqItems);
        CurrSizeFiRanks preprocessedF2 = CurrSizeFiRanks.construct(f2, totalFreqItems, totalFreqItems, f2Ranks);
//        pp("zzz");
//        List<Integer[]> f3AsArrays = apr.computeFk(filteredTrs, preprocessedF2);
        JavaRDD<Tuple2<int[], int[]>> ranks1And2 = apr.toRddOfRanks1And2(filteredTrs, preprocessedF2);
        ranks1And2 = ranks1And2.persist(StorageLevel.MEMORY_ONLY_SER());
        pp("zzz");
        List<int[]> f3AsArrays = apr.computeFk(3, ranks1And2, preprocessedF2);
        pp("F3 as arrays size: "+f3AsArrays.size());
        List<int[]> f3 = apr.fkAsArraysToRankPairs(f3AsArrays);
        pp("F3 size: "+f3.size());
        List<FreqItemset<String>> f3Res = apr.fkAsArraysToResItemsets(f3AsArrays, itemToRank, preprocessedF2);
        f3Res = f3Res.stream().sorted((fi1, fi2) -> Integer.compare(fi2.freq, fi1.freq)).collect(Collectors.toList());
        pp("F3: " + StringUtils.join(f3Res.subList(0, Math.min(50, f3Res.size())), "\n"));

        CurrSizeFiRanks preprocessedF3 = CurrSizeFiRanks.construct(f3, totalFreqItems, f2.size(), f2Ranks);
        JavaRDD<Tuple2<int[], int[]>> ranks1And3 = apr.toRddOfRanks1AndK(ranks1And2, preprocessedF3);

        TidsGenHelper tidsGenHelper = preprocessedF3.constructTidGenHelper(f3, (int)prep.totalTrs);
        JavaRDD<long[]> tidsRdd = apr.toRddOfTidsNew(ranks1And3, tidsGenHelper, prep.totalTrs);
//        List<List<Long>> allTids = apr.tmpToListOfTidListsNew(tidsRdd, 100).collect();
        List<List<Long>> allTids = apr.tmpToTidCnts(tidsRdd).collect();
//        JavaRDD<long[]> tidsRdd = apr.toRddOfTids(ranks1And3, tidsGenHelper);
//        List<List<Long>> allTids = apr.tmpToListOfTidLists(tidsRdd).collect();
        pp("TIDs:");
        for (List<Long> tids : allTids) {
            System.out.println(tids);
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
