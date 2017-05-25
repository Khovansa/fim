package org.openu.fimcmp.algs.fpgrowth;

import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.junit.Before;
import org.junit.Test;
import org.openu.fimcmp.AlgITBase;
import org.openu.fimcmp.itemset.FreqItemset;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Test of standard FPGrowth Spark implementation
 */
public class FpGrowthStdIT extends AlgITBase {
    @Before
    public void setUp() throws Exception {
        setUpRun(true);
    }

    @Test
    public void test() {
        final double minSupp = 0.8;
//        final PrepStepOutputAsList prep = prepareAsList("my.small.txt", minSupp, false, 1);
        final PrepStepOutputAsList prep = prepareAsList("pumsb.dat", minSupp, false, 1);

        FPGrowth fpg = new FPGrowth()
                .setMinSupport(minSupp)
                .setNumPartitions(prep.trs.getNumPartitions());
        FPGrowthModel<String> model = fpg.run(prep.trs);

        List<FPGrowth.FreqItemset<String>> resAsList = model.freqItemsets().toJavaRDD().collect();
        pp("Total results: " + resAsList.size());
        List<FreqItemset> stdRes = resAsList.stream().
                map(fi -> new FreqItemset(fi.javaItems(), (int)fi.freq())).
                sorted(FreqItemset::compareForNiceOutput2).collect(Collectors.toList());
        for (FreqItemset fi : stdRes) {
            System.out.println(fi);
        }
        pp("Total results: " + stdRes.size());
    }
}
