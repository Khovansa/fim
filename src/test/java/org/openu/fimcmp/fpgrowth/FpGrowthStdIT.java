package org.openu.fimcmp.fpgrowth;

import org.openu.fimcmp.AlgITBase;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Test of standard FPGrowth Spark implementation
 */
public class FpGrowthStdIT extends AlgITBase {
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void test() {
        final double minSupp = 0.8;
//        final PrepStepOutput prep = prepare("my.small.txt", minSupp);
        final PrepStepOutput prep = prepare("pumsb.dat", minSupp, false);

        FPGrowth fpg = new FPGrowth()
                .setMinSupport(minSupp)
                .setNumPartitions(1);
        FPGrowthModel<String> model = fpg.run(prep.trs);

        List<FPGrowth.FreqItemset<String>> resAsList = model.freqItemsets().toJavaRDD().collect();
        pp("Total results: "+resAsList.size());
        List<FPGrowth.FreqItemset<String>> f3s = resAsList.stream()
                .filter(fi -> fi.javaItems().size() == 3)
                .sorted((fi1, fi2) -> Long.compare(fi2.freq(), fi1.freq()))
                .collect(Collectors.toList());
        pp("F3 size: "+f3s.size());
        for (FPGrowth.FreqItemset<String> itemset : f3s.subList(0, Math.min(100, f3s.size()))) {
            System.out.println("[" + itemset.javaItems() + "], " + itemset.freq());
        }
    }
}
