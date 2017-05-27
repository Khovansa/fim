package org.openu.fimcmp.algs.fpgrowth;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.apache.spark.rdd.RDD;
import org.openu.fimcmp.algs.algbase.AlgBase;
import org.openu.fimcmp.cmdline.CmdLineOptions;
import org.openu.fimcmp.itemset.FreqItemset;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The main class for the FpGrowth algorithm, <b>just invokes the standard Spark implementation.</b>
 */
public class FpGrowthAlg extends AlgBase<FpGrowthAlgProperties, FPGrowthModel<String>> {

    //--spark-master-url local --input-file-name pumsb.dat --min-supp 0.8 --input-parts-num 1 --persist-input true --cnt-only true --print-all-fis false
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
    public FPGrowthModel<String> run(JavaSparkContext sc, StopWatch sw) throws Exception {
        JavaRDD<ArrayList<String>> trs = readInputAsListRdd(sc, sw);

        FPGrowth fpg = new FPGrowth()
                .setMinSupport(props.minSupp)
                .setNumPartitions(trs.getNumPartitions());

        pp(sw, "Starting FI computation");
        FPGrowthModel<String> model = fpg.run(trs);
        RDD<FPGrowth.FreqItemset<String>> freqItemsetRDD = model.freqItemsets();

        outputResults(freqItemsetRDD, sw);

        return model;
    }

    private void outputResults(RDD<FPGrowth.FreqItemset<String>> freqItemsetRDD, StopWatch sw) {
        if (props.isCountingOnly) {
            pp(sw, "Total results: " + freqItemsetRDD.count());
        } else {
            List<FPGrowth.FreqItemset<String>> resAsList = freqItemsetRDD.toJavaRDD().collect();
            pp(sw, "Total results: " + resAsList.size());
            if (props.isPrintAllFis) {
                List<FreqItemset> stdRes = resAsList.stream()
                        .map(fi -> new FreqItemset(fi.javaItems(), (int)fi.freq()))
                        .collect(Collectors.toList());
                stdRes = printAllItemsets(stdRes);
                pp(sw, "Total results: " + stdRes.size());
            }
        }
    }
}
