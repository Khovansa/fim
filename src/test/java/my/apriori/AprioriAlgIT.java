package my.apriori;

import my.AlgITBase;
import my.BasicOps;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
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
    public void test() {
        final PrepStepOutput prep = prepare("kosarak.dat", 0.1);
//        final PrepStepOutput prep = prepare("my.small.txt", 0.06);
//        final PrepStepOutput prep = prepare("kosarak.dat", 0.06);
//        final PrepStepOutput prep = prepare("pumsb.dat", 0.15);

        apr = new AprioriAlg<>(prep.minSuppCount);
        JavaPairRDD<String, Integer> f1AsRdd = apr.computeF1(prep.trs);
        final HashSet<String> f1AsFastSet = basicOps.fillCollectionFromRdd(new HashSet<>(), f1AsRdd);
        exploreF1(f1AsRdd);

        //2-FIs
        JavaRDD<ArrayList<String>> filteredTrs = prep.trs
                .map(tr -> BasicOps.withoutInfrequent(tr, f1AsFastSet))
//                .persist(StorageLevel.MEMORY_ONLY())
                ;
        pp("Filtered");
        JavaRDD<Collection<List<String>>> cand2AsRdd = apr.computeCand2(filteredTrs);
        JavaPairRDD<List<String>, Integer> f2AsRdd = apr.countAndFilterByMinSupport(cand2AsRdd);
        pp("RDD computed: "+f2AsRdd.count());
        Collection<List<String>> f2 = apr.toCollectionOfLists(f2AsRdd);
        exploreFk(f2AsRdd, f2, 2);

        //3-FIs
        Set<String> f1AsFastSet2 = apr.getUpdatedF1(f1AsFastSet, f2);
        Set<List<String>> f2AsSet = new HashSet<>(f2);
        pp("F1 size = " + f1AsFastSet2.size());
        cand2AsRdd = cand2AsRdd
                .map(tr -> (Collection<List<String>>)BasicOps.withoutInfrequent(tr, f2AsSet))
                .filter(tr -> tr.size() >= 3)
                .persist(StorageLevel.MEMORY_ONLY())
        ;
        JavaRDD<Collection<List<String>>> cand3AsRdd =
                apr.computeNextSizeCandidates(cand2AsRdd, 2, f1AsFastSet2, f2AsSet);
        JavaPairRDD<List<String>, Integer> f3AsRdd = apr.countAndFilterByMinSupport(cand3AsRdd);
        pp("RDD computed: "+f3AsRdd.count());
        Collection<List<String>> f3 = apr.toCollectionOfLists(f3AsRdd);
        exploreFk(f3AsRdd, f3, 3);
    }
}