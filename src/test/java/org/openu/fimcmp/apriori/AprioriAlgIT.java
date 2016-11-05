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
        final PrepStepOutputAsArr prep = prepareAsArr("pumsb.dat", 0.8, false);
        apr = new AprioriAlg<>(prep.minSuppCount);
        List<String> sortedF1 = apr.computeF1(prep.trs);
        pp("F1 size = " + sortedF1.size());
        pp(sortedF1);
        Map<String, Integer> itemToRank = BasicOps.itemToRank(sortedF1);
        //from now on, the items are [0, sortedF1.size), 0 denotes the most frequent item

        //TODO: keep the partition of the transaction!
        JavaRDD<int[]> filteredTrs = prep.trs.map(t -> BasicOps.getMappedFilteredAndSortedTrs2(t, itemToRank));
        filteredTrs = filteredTrs.persist(StorageLevel.MEMORY_ONLY_SER());
        pp("filtered and saved");

        List<int[]> f2AsArrays = apr.computeF2(filteredTrs);
        pp("F2 as arrays size: "+f2AsArrays.size());
        List<int[]> f2 = apr.f2AsArraysToRankPairs(f2AsArrays);
        pp("F2 size: "+f2.size());
//        List<FreqItemset<String>> f2Res = apr.f2AsArraysToPairs(f2AsArrays, itemToRank);
//        pp("F2: "+StringUtils.join(f2Res.subList(0, Math.min(100, f2Res.size())), "\n"));

        PreprocessedF2 preprocessedF2 = PreprocessedF2.construct2(f2, sortedF1.size());
//        pp("zzz");
//        List<Integer[]> f3AsArrays = apr.computeF3(filteredTrs, preprocessedF2);
        JavaRDD<Tuple2<int[], int[]>> ranks1And2 = apr.toRddOfRanks1And2(filteredTrs, preprocessedF2);
        ranks1And2 = ranks1And2.persist(StorageLevel.MEMORY_ONLY_SER());
        pp("zzz");
        List<int[]> f3AsArrays = apr.computeF3(ranks1And2, preprocessedF2);
        pp("F3 as arrays size: "+f3AsArrays.size());
        List<FreqItemset<String>> f3 = apr.f3AsArraysToTriplets(f3AsArrays, itemToRank, preprocessedF2);
        pp("F3 size: "+f3.size());
        f3 = f3.stream().sorted((fi1, fi2) -> Integer.compare(fi2.freq, fi1.freq)).collect(Collectors.toList());
        pp("F3: " + StringUtils.join(f3.subList(0, Math.min(3, f3.size())), "\n"));
    }
}