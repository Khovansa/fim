package org.openu.fimcmp.fpgrowth;

import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.junit.Before;
import org.junit.Test;
import org.openu.fimcmp.AlgITBase;

import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;
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
        final PrepStepOutputAsList prep = prepareAsList("pumsb.dat", minSupp, false, 3);

        FPGrowth fpg = new FPGrowth()
                .setMinSupport(minSupp)
                .setNumPartitions(prep.trs.getNumPartitions());
        FPGrowthModel<String> model = fpg.run(prep.trs);

        List<FPGrowth.FreqItemset<String>> resAsList = model.freqItemsets().toJavaRDD().collect();
        pp("Total results: " + resAsList.size());
        List<FPGrowth.FreqItemset<String>> f3s = resAsList.stream()
                .filter(fi -> fi.javaItems().size() == 4)
                .sorted((fi1, fi2) -> Long.compare(fi2.freq(), fi1.freq()))
                .collect(Collectors.toList());
        pp("F4 size: " + f3s.size());
        for (FPGrowth.FreqItemset<String> itemset : f3s.subList(0, Math.min(1000, f3s.size()))) {
            Object[] items = (Object[]) itemset.items();
            List<Object> ts = Arrays.asList(items);
            if (ts.contains("86") && ts.contains("59")) {
                System.out.println("[" + new TreeSet<>(itemset.javaItems()) + "], " + itemset.freq());
            }
        }
    }
}
