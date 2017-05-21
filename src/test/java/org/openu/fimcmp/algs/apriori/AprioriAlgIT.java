package org.openu.fimcmp.algs.apriori;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import org.junit.Before;
import org.junit.Test;
import org.openu.fimcmp.*;
import org.openu.fimcmp.algs.algbase.BasicOps;
import org.openu.fimcmp.algs.eclat.EclatAlg;
import org.openu.fimcmp.algs.eclat.EclatProperties;
import org.openu.fimcmp.result.FiResultHolder;
import org.openu.fimcmp.util.BitArrays;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class AprioriAlgIT extends AlgITBase implements Serializable {
    @SuppressWarnings("FieldCanBeLocal")
    private AprioriAlg<String> apr;
    @SuppressWarnings("FieldCanBeLocal")
    private EclatAlg eclat;
    private Map<String, Integer> itemToRank;
    private String[] rankToItem;

    @Before
    public void setUp() throws Exception {
        setUpRun(false);
    }

    @Test
    public void test() throws Exception {
        runIt();
//        Thread.sleep(100_000_000);
    }

    private void runIt() throws Exception {
        final Integer maxEclatNumParts = 1;
        final PrepStepOutputAsArr prep = prepareAsArr("pumsb.dat", 0.4, false, 1);
//        final PrepStepOutputAsArr prep = prepareAsArr("my.small.txt", 0.3, false, 1);
        apr = new AprioriAlg<>(prep.minSuppCount);

        List<String> sortedF1 = apr.computeF1(prep.trs);
        final int totalFreqItems = sortedF1.size();
        pp("F1 size = " + totalFreqItems);
        pp(sortedF1);
        itemToRank = BasicOps.itemToRank(sortedF1);
        rankToItem = BasicOps.getRankToItem(itemToRank);
        EclatProperties eclatProps = new EclatProperties(prep.minSuppCount, totalFreqItems);
        eclatProps.setUseDiffSets(true);
        eclatProps.setSqueezingEnabled(false);
        eclatProps.setCountingOnly(true);
        eclatProps.setRankToItem(rankToItem);
        eclat = new EclatAlg(eclatProps);
        //from now on, the items are [0, sortedF1.size), 0 denotes the most frequent item

        JavaRDD<int[]> filteredTrs = prep.trs.map(t -> BasicOps.getMappedFilteredAndSortedTrs(t, itemToRank));
        filteredTrs = filteredTrs.persist(StorageLevel.MEMORY_ONLY_SER());
//        filteredTrs = filteredTrs.persist(StorageLevel.MEMORY_AND_DISK_SER());
        pp("filtered and saved");

        List<int[]> f2AsArrays = apr.computeF2_Part(filteredTrs, totalFreqItems);
        pp("F2 as arrays size: " + f2AsArrays.size());
        List<int[]> f2 = apr.fkAsArraysToRankPairs(f2AsArrays);
        pp("F2 size: " + f2.size());
        FiRanksToFromItems fiRanksToFromItemsR1 = new FiRanksToFromItems();
        List<FreqItemset> f2Res =
                apr.fkAsArraysToResItemsets(f2AsArrays, rankToItem, fiRanksToFromItemsR1);
        f2Res = f2Res.stream().sorted((fi1, fi2) -> Integer.compare(fi2.freq, fi1.freq)).collect(Collectors.toList());
        pp("F2: " + StringUtils.join(f2Res.subList(0, Math.min(3, f2Res.size())), "\n"));

        CurrSizeFiRanks preprocessedF2 = CurrSizeFiRanks.construct(f2, totalFreqItems, totalFreqItems);
        FiRanksToFromItems fiRanksToFromItemsR2 = fiRanksToFromItemsR1.toNextSize(preprocessedF2);
        JavaRDD<Tuple2<int[], long[]>> ranks1And2 = apr.toRddOfRanks1And2(filteredTrs, preprocessedF2);
        ranks1And2 = ranks1And2.persist(StorageLevel.MEMORY_ONLY_SER());
//        ranks1And2 = ranks1And2.persist(StorageLevel.MEMORY_AND_DISK_SER());
        pp("zzz");
        NextSizeItemsetGenHelper f3GenHelper = NextSizeItemsetGenHelper.construct(
                fiRanksToFromItemsR2, totalFreqItems, f2.size());
        List<int[]> f3AsArrays = apr.computeFk_Part(3, ranks1And2, f3GenHelper);
        pp("F3 as arrays size: " + f3AsArrays.size());
        List<int[]> f3 = apr.fkAsArraysToRankPairs(f3AsArrays);
        pp("F3 size: " + f3.size());
        List<FreqItemset> f3Res = apr.fkAsArraysToResItemsets(f3AsArrays, rankToItem, fiRanksToFromItemsR2);
        f3Res = f3Res.stream().sorted((fi1, fi2) -> Integer.compare(fi2.freq, fi1.freq)).collect(Collectors.toList());
        pp("F3: " + StringUtils.join(f3Res.subList(0, Math.min(10, f3Res.size())), "\n"));

        CurrSizeFiRanks preprocessedF3 = CurrSizeFiRanks.construct(f3, totalFreqItems, f2.size());
        JavaRDD<Tuple2<int[], long[]>> ranks1And3 = apr.toRddOfRanks1AndK(ranks1And2, preprocessedF3);
//        ranks1And3 = ranks1And3.persist(StorageLevel.MEMORY_AND_DISK_SER());

        TidsGenHelper tidsGenHelper = preprocessedF3.constructTidGenHelper(f3, (int) prep.totalTrs);

        JavaRDD<long[]> kRanksBsRdd = ranks1And3.map(r1And3 -> r1And3._2);
        filteredTrs.unpersist();
        kRanksBsRdd = kRanksBsRdd.persist(StorageLevel.MEMORY_AND_DISK_SER());
//        ranks1And2.unpersist();
        FiRanksToFromItems fiRanksToFromItemsR3 = fiRanksToFromItemsR2.toNextSize(preprocessedF3);
        PairRanks r3ToR2AndR1 = fiRanksToFromItemsR3.constructRkToRkm1AndR1ForMaxK();

        pp("Starting collecting the TIDs");
        JavaRDD<long[][]> rankToTidBsRdd = apr.computeCurrRankToTidBitSet_Part(kRanksBsRdd, tidsGenHelper);
        rankToTidBsRdd = rankToTidBsRdd.persist(StorageLevel.MEMORY_AND_DISK_SER());
//        long[][] rankKToTids = apr.mergePartitions(rankToTidBsRdd, tidsGenHelper);
        ranks1And2.unpersist();
        kRanksBsRdd.unpersist();

//        printSomeTids(rankKToTids);

        int eclatNumParts =
                AprioriAlg.getNumPartsForEclat(rankToTidBsRdd.getNumPartitions(), r3ToR2AndR1, maxEclatNumParts)._1;
        JavaPairRDD<Integer, List<long[]>> r2ToTidSets =
                apr.groupTidSetsByRankKm1(rankToTidBsRdd, r3ToR2AndR1, eclatNumParts);
        JavaPairRDD<Integer, ItemsetAndTidsCollection> r2ToEclatInput = r2ToTidSets
                .mapValues(tidSets ->
                        TidMergeSet.mergeTidSetsWithSameRankDropMetadata(tidSets, tidsGenHelper, fiRanksToFromItemsR3));
//        exploreFirstEclatInputElem(r2ToEclatInput);

        pp("Starting Eclat computations");
        JavaRDD<FiResultHolder> resRdd = eclat.computeFreqItemsetsRdd(r2ToEclatInput);
        pp("Num parts: " + r2ToTidSets.getNumPartitions());
        pp("Num parts (res): " + resRdd.getNumPartitions());
        //TODO: actual results generation is here:
//        JavaPairRDD<Integer, int[]> resRdd2 = resRdd.flatMap(List::iterator)
//                .mapToPair(p -> new Tuple2<>(FreqItemsetAsRanksBs.extractSupportCnt(p), FreqItemsetAsRanksBs.extractItemset(p)));
////        List<Tuple2<Integer, int[]>> eclatRes = resRdd2.sortByKey(false).collect();
//        pp("RES SIZE: " + resRdd.count());
//        List<Tuple2<Integer, int[]>> eclatRes = resRdd2.sortByKey(false).collect();
        final int prevResCnt = totalFreqItems + f2.size() + f3.size();
//        printEclatRes1(eclatRes, itemToRank, prevResCnt);
        //--------------
        //counting only
        Long eclatTotalRes = resRdd.map(FiResultHolder::size).reduce((x, y) -> x + y);
        pp(String.format("ZZZ: total res count=%s, Eclat only=%s", (eclatTotalRes+prevResCnt), eclatTotalRes));

//        printSomeR2ToR3(fiRanksToFromItemsR3, r2ToTidSets);

//        pp("zzz");
//        List<int[]> f4AsArrays = apr.computeFk_Part(4, ranks1And3, preprocessedF3);
//        pp("F4 as arrays size: "+f4AsArrays.size());
//        List<int[]> f4 = apr.fkAsArraysToRankPairs(f4AsArrays);
//        pp("F4 size: "+f4.size());
//        List<FreqItemset<String>> f4Res =
//          apr.fkAsArraysToResItemsets(f4AsArrays, itemToRank, new new FiRanksToFromItems(preprocessedF3, preprocessedF2));
//        f4Res = f4Res.stream().sorted((fi1, fi2) -> Integer.compare(fi2.freq, fi1.freq)).collect(Collectors.toList());
//        pp("F4: " + StringUtils.join(f4Res.subList(0, Math.min(3, f4Res.size())), "\n"));
    }

    private void printSomeR2ToR3(FiRanksToFromItems fiRanksToFromItemsR3, JavaPairRDD<Integer, List<long[]>> r2ToTidSets) {
        pp("r2 -> r3s:");
        List<Tuple2<List<String>, List<List<String>>>> r2ToR3sList = r2ToTidSets.sortByKey()
                .map(t -> new Tuple2<>(
                        fiRanksToFromItemsR3.getOrigItemsetByRank(t._1, 2, rankToItem),
                        fiRanksToFromItemsR3.toOrigItemsetsForDebug(t._2, 3, rankToItem, 30)))
                .collect().subList(0, 20);
        for (Tuple2<List<String>, List<List<String>>> r2ToR3s : r2ToR3sList) {
            System.out.println(String.format("%-20s -> %s", r2ToR3s._1, r2ToR3s._2));
        }
    }

    private void exploreFirstEclatInputElem(JavaPairRDD<Integer, ItemsetAndTidsCollection> r2ToEclatInput) {
        ItemsetAndTidsCollection eclatInput1 = r2ToEclatInput.sortByKey().first()._2;
        pp("Itemset size: " + eclatInput1.getItemsetSize());
        pp("Total tids: " + eclatInput1.getTotalTids());
        pp("Elems size: " + eclatInput1.getObjArrayListCopy().size());
        ItemsetAndTids firstInputEclatElem = eclatInput1.getObjArrayListCopy().get(0);
        pp("Support cnt #1: " + firstInputEclatElem.getSupportCount());
        pp("Support cnt #1 (cardinality): " + BitArrays.cardinality(firstInputEclatElem.getTidBitSet(), 0));

        pp("Itemset #1: " + firstInputEclatElem.toOrigItemsetForDebug(rankToItem));
//        pp("TIDs of itemset #1: " + Arrays.toString(BitArrays.asNumbers(firstEclatElem.getTidBitSet(), 0)));
//        List<Tuple2<int[], Integer>> resEclat1 = eclat.computeFreqItemsetsSingle(eclatInput1);
//        printEclatRes2(resEclat1, itemToRank);
    }

    private void printSomeTids(long[][] rankKToTids) {
        pp("TIDs:");
        for (int rankK = 0, cnt = 0; rankK < 500 && cnt < 20; ++rankK) {
            if (rankKToTids[rankK] != null) {
                ++cnt;
                System.out.println(Arrays.toString(TidMergeSet.describeAsList(rankKToTids[rankK])));
            }
        }
    }

    private void printEclatRes1(List<Tuple2<Integer, int[]>> eclatRes, Map<String, Integer> itemToRank, int prevResCnt) {
        int eclatResCnt = eclatRes.size();
        pp(String.format("Result count: %s (Eclat only: %s)", (prevResCnt + eclatResCnt), eclatResCnt));
        for (Tuple2<Integer, int[]> fiAndSupport : eclatRes.subList(0, 10)) {
            FreqItemset fi = FreqItemset.constructFromRanks(fiAndSupport._2, fiAndSupport._1, rankToItem);
//            if (fi.containsItems("86", "10")) { //86=0, 10=17, 23=11
                System.out.println(fi.toString(itemToRank, 10));
//            }
        }
    }

    private void printEclatRes2(List<Tuple2<int[], Integer>> eclatRes, Map<String, Integer> itemToRank) {
        pp("Eclat res: " + eclatRes.size());
        for (Tuple2<int[], Integer> fiAndSupport : eclatRes.subList(0, 20)) {
            FreqItemset fi = FreqItemset.constructFromRanks(fiAndSupport._1, fiAndSupport._2, rankToItem);
            System.out.println(fi.toString(itemToRank, 10));
        }
    }
}
