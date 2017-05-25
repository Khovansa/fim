package org.openu.fimcmp.algs.fpgrowth;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.apache.spark.rdd.RDD;
import org.openu.fimcmp.algs.algbase.AlgBase;
import org.openu.fimcmp.cmdline.CmdLineOptions;

import java.util.ArrayList;

/**
 * The main class for the FpGrowth algorithm, <b>just invokes the standard Spark implementation.</b>
 */
public class FpGrowthAlg extends AlgBase<FpGrowthAlgProperties, Void> {

    public static void main(String[] args) throws Exception {
        FpGrowthCmdLineOptionsParser cmdLineOptionsParser = new FpGrowthCmdLineOptionsParser();
        CmdLineOptions<FpGrowthAlgProperties> runProps = cmdLineOptionsParser.parseCmdLine(args);

        if (runProps == null) {
            return; //help
        }

        StopWatch sw = new StopWatch();
        sw.start();
        pp(sw, runProps);
        JavaSparkContext sc = createSparkContext(runProps.isUseKrio, runProps.sparkMasterUrl, sw);

        FpGrowthAlg alg = cmdLineOptionsParser.createAlg(runProps);
        alg.run(sc, sw);
    }

    public static Class[] getClassesToRegister() {
        return new Class[]{
                org.apache.spark.mllib.fpm.FPTree.class,
                org.apache.spark.mllib.fpm.FPTree.Node.class,
                /*org.apache.spark.mllib.fpm.FPTree.Summary.class, */
                scala.collection.mutable.ListBuffer.class,
                org.apache.spark.mllib.fpm.FPGrowth.FreqItemset.class,
                org.apache.spark.mllib.fpm.FPGrowth.FreqItemset[].class,
        };
    }

    @SuppressWarnings("WeakerAccess")
    public FpGrowthAlg(FpGrowthAlgProperties props, String inputFile) {
        super(props, inputFile);
    }

    @Override
    public Void run(JavaSparkContext sc, StopWatch sw) throws Exception {
        JavaRDD<ArrayList<String>> trs = readInputAsListRdd(sc, sw);

        FPGrowth fpg = new FPGrowth()
                .setMinSupport(props.minSupp)
                .setNumPartitions(trs.getNumPartitions());
        FPGrowthModel<String> model = fpg.run(trs);
        RDD<FPGrowth.FreqItemset<String>> freqItemsetRDD = model.freqItemsets();

        //TODO
        
        return null;
    }
}
