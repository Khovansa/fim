package my.apriori;

import my.AlgITBase;
import my.BasicOps;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

public class AprioriAlgIT extends AlgITBase {
    @SuppressWarnings("FieldCanBeLocal")
    private AprioriAlg<String> apr;

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void test() throws InterruptedException {
        final PrepStepOutput prep = prepare("pumsb.dat", 0.8);
//        final PrepStepOutput prep = prepare("my.small.txt", 0.06);
//        final PrepStepOutput prep = prepare("kosarak.dat", 0.06);
//        final PrepStepOutput prep = prepare("pumsb.dat", 0.15);

        apr = new AprioriAlg<>(prep.minSuppCount);
        JavaPairRDD<String, Integer> f1AsRdd = apr.computeF1(prep.trs);
        Broadcast<HashSet<String>> f1AsFastSetBr =
                sc.broadcast(basicOps.fillCollectionFromRdd(new HashSet<>(), f1AsRdd));
        exploreF1(f1AsRdd);
        long totalItems = prep.trs.flatMap(ArrayList::iterator).count();

        //2-FIs
        JavaRDD<ArrayList<String>> filteredTrs = prep.trs
                .map(tr -> BasicOps.withoutInfrequent(tr, f1AsFastSetBr.value()))
                .persist(StorageLevel.MEMORY_ONLY_SER())
                ;
        pp("Just after 'persist' of filtered Trs[1]");
        long filteredCnt = filteredTrs.flatMap(ArrayList::iterator).count();
        pp(String.format("Filtered: %s, items per transaction: %.2f, filtered items per transaction: %.2f",
                filteredCnt, 1.0*totalItems/prep.totalTrs, 1.0*filteredCnt/prep.totalTrs));
        JavaRDD<Collection<List<String>>> cand2AsRdd = apr.computeCand2(filteredTrs);
//        long candPairsCnt = cand2AsRdd.flatMap(Collection::iterator).count();
//        pp(String.format("Cand pairs per transaction: %.2f", 1.0*candPairsCnt/prep.totalTrs));
        JavaPairRDD<List<String>, Integer> f2AsRdd = apr.countAndFilterByMinSupport(cand2AsRdd);
        pp(String.format("RDD of pairs computed: %s", f2AsRdd.count()));
        Collection<List<String>> f2 = apr.toCollectionOfLists(f2AsRdd);
        exploreFk(f2AsRdd, f2, 2);

        //3-FIs
        Broadcast<Set<String>> f1AsFastSet2 = sc.broadcast(apr.getUpdatedF1(f1AsFastSetBr.value(), f2));
        Broadcast<Set<List<String>>> f2AsSet = sc.broadcast(new HashSet<>(f2));
        pp("F1 size = " + f1AsFastSet2.value().size());
        cand2AsRdd = cand2AsRdd
                .map(tr -> (Collection<List<String>>)BasicOps.withoutInfrequent(tr, f2AsSet.value()))
                .filter(tr -> tr.size() >= 3)
        ;
        long filteredCandPairsCnt = cand2AsRdd.flatMap(Collection::iterator).count();
        pp(String.format("Filtered cand pairs per transaction: %.2f", 1.0*filteredCandPairsCnt/prep.totalTrs));
        cand2AsRdd = cand2AsRdd.persist(StorageLevel.MEMORY_ONLY_SER());
        pp("Persisted cand2AsRdd");
        JavaRDD<Collection<List<String>>> cand3AsRdd =
                apr.computeNextSizeCandidates(cand2AsRdd, 2, f1AsFastSet2.value(), f2AsSet.value());
        JavaPairRDD<List<String>, Integer> f3AsRdd = apr.countAndFilterByMinSupport(cand3AsRdd);
        pp("RDD computed: "+f3AsRdd.count());
        Collection<List<String>> f3 = apr.toCollectionOfLists(f3AsRdd);
        exploreFk(f3AsRdd, f3, 3);
    }
}