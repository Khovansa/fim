package org.openu.fimcmp.bigfim;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.openu.fimcmp.algbase.AlgBase;
import org.openu.fimcmp.props.CmdLineOptions;
import org.openu.fimcmp.result.FiResultHolder;
import scala.Tuple2;


/**
 * The main class that implements the Big FIM algorithm.
 */
public class BigFimAlg extends AlgBase<BigFimAlgProperties, BigFimResult> {

    public static void main(String[] args) throws Exception {
        BigFimCmdLineOptionsParser cmdLineOptionsParser = new BigFimCmdLineOptionsParser();
        CmdLineOptions<BigFimAlgProperties> runProps = cmdLineOptionsParser.parseCmdLine(args);
        if (runProps == null) {
            return; //help
        }

        StopWatch sw = new StopWatch();
        sw.start();
        pp(sw, runProps);
        JavaSparkContext sc = createSparkContext(runProps.isUseKrio, runProps.sparkMasterUrl, sw);

        BigFimAlg alg = cmdLineOptionsParser.createAlg(runProps);
        alg.run(sc, sw);
    }

    public BigFimAlg(BigFimAlgProperties props, String inputFile) {
        super(props, inputFile);
    }

    @Override
    public BigFimResult run(JavaSparkContext sc, StopWatch sw) throws Exception {
        JavaRDD<String[]> trs = readInput(sc, sw);

        BigFimResult res = computeFis(trs, sw);
        res.printCounts(sw);

        return res;
    }

    private BigFimResult computeFis(JavaRDD<String[]> trs, StopWatch sw) {
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
