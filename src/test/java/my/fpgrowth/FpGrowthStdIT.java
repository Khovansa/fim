package my.fpgrowth;

import my.AlgITBase;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

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
        final PrepStepOutput prep = prepare("pumsb.dat", minSupp);

        FPGrowth fpg = new FPGrowth()
                .setMinSupport(minSupp)
                .setNumPartitions(1);
        FPGrowthModel<String> model = fpg.run(prep.trs);

        List<FPGrowth.FreqItemset<String>> resAsList = model.freqItemsets().toJavaRDD().collect();
        pp("Total results: "+resAsList.size());
        int cnt = 0;
        for (FPGrowth.FreqItemset<String> itemset : resAsList) {
            if (cnt > 1000) {
                break;
            }
            ++cnt;
            System.out.println("[" + itemset.javaItems() + "], " + itemset.freq());
        }
    }
}
