package org.openu.fimcmp.bigfim;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.openu.fimcmp.algbase.AlgBase;
import org.openu.fimcmp.result.FiResultHolder;
import scala.Tuple2;


/**
 * The main class that implements the Big FIM algorithm.
 */
public class BigFimAlg extends AlgBase<BigFimAlgProperties> {
    public static void main(String[] args) throws Exception {
        BigFimCmdLineOptions runProps = BigFimCmdLineOptions.parse(args);
        if (runProps == null) {
            return; //help
        }

        BigFimAlgProperties props = runProps.bigFimAlgProps;
        BigFimAlg alg = new BigFimAlg(props);

        StopWatch sw = new StopWatch();
        sw.start();
        pp(sw, runProps);
        JavaSparkContext sc = createSparkContext(runProps.isUseKrio, runProps.sparkMasterUrl, sw);

        String inputFile = "C:\\Users\\Alexander\\Desktop\\Data Mining\\DataSets\\" + runProps.inputFileName;
        JavaRDD<String[]> trs = alg.readInput(sc, inputFile, sw);

        BigFimResult res = alg.computeFis(trs, sw);
        res.printCounts(sw);

    }
    public BigFimAlg(BigFimAlgProperties props) {
        super(props);
    }

    public BigFimResult computeFis(JavaRDD<String[]> trs, StopWatch sw) {
        BigFimStepExecutor helper = new BigFimStepExecutor(props, computeF1Context(trs, sw));

        JavaRDD<int[]> ranks1Rdd = helper.computeRddRanks1(trs);
        AprioriStepRes currStep = helper.computeF2(ranks1Rdd);

        JavaRDD<Tuple2<int[], long[]>> ranks1AndK = null;
        while (currStep != null && helper.isContinueWithApriori()) {
            ranks1AndK = helper.computeCurrSizeRdd(currStep, ranks1AndK, ranks1Rdd, false);
            currStep = helper.computeFk(ranks1AndK, currStep);
        }

        JavaRDD<FiResultHolder> optionalEclatFis = null;
        if (currStep != null && ranks1AndK != null && helper.canContinue()) {
            ranks1AndK = helper.computeCurrSizeRdd(currStep, ranks1AndK, ranks1Rdd, true);
            optionalEclatFis = helper.computeWithEclat(currStep, ranks1AndK);
        }

        return helper.createResult(optionalEclatFis);
    }
}
