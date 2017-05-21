package org.openu.fimcmp.cmdline;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.spark.api.java.JavaSparkContext;
import org.openu.fimcmp.algs.algbase.AlgBase;
import org.openu.fimcmp.algs.algbase.CommonAlgProperties;
import org.openu.fimcmp.algs.bigfim.BigFimCmdLineOptionsParser;
import org.openu.fimcmp.algs.fin.FinCmdLineOptionsParser;
import org.openu.fimcmp.util.Assert;

import java.util.Map;
import java.util.TreeMap;

/**
 * The main class of the 'fim-cmp' project and the main entry point. <br/>
 * Parses the command-line options and dispatches the execution to the appropriate algorithm.
 */
@SuppressWarnings("WeakerAccess")
public class CmdLineRunner {
    private final Map<String, ICmdLineOptionsParser<? extends CommonAlgProperties, ? extends AlgBase>> algNameToAlgOptionsParser;

    private static Map<String, ICmdLineOptionsParser<? extends CommonAlgProperties, ? extends AlgBase>> defaultAlgNameToAlgOptionsParser() {
        Map<String, ICmdLineOptionsParser<? extends CommonAlgProperties, ? extends AlgBase>> res = new TreeMap<>();
        res.put("BIG_FIM", new BigFimCmdLineOptionsParser());
        res.put("FIN", new FinCmdLineOptionsParser());
        return res;
    }


    public static void main(String[] args) throws Exception {
        CmdLineRunner runner = new CmdLineRunner();
        runner.run(args);
    }


    public CmdLineRunner() {
        this(defaultAlgNameToAlgOptionsParser());
    }

    public CmdLineRunner(Map<String, ICmdLineOptionsParser<? extends CommonAlgProperties, ? extends AlgBase>> algNameToAlgOptionsParser) {
        Assert.isTrue(algNameToAlgOptionsParser != null && !algNameToAlgOptionsParser.isEmpty());
        this.algNameToAlgOptionsParser = algNameToAlgOptionsParser;
    }

    public void run(String[] args) throws Exception {
        ICmdLineOptionsParser<? extends CommonAlgProperties, ? extends AlgBase> cmdLineOptionsParser =
                findCmdLineOptionsParser(args);
        if (cmdLineOptionsParser == null) {
            return; //--help
        }

        String[] algArgs = ArrayUtils.subarray(args, 1, args.length);
        CmdLineOptions<? extends CommonAlgProperties> runProps = cmdLineOptionsParser.parseCmdLine(algArgs);
        if (runProps == null) {
            return; //<alg> --help, i.e. help on alg-specific options
        }

        StopWatch sw = new StopWatch();
        sw.start();
        AlgBase.pp(sw, runProps);
        JavaSparkContext sc = AlgBase.createSparkContext(runProps.isUseKrio, runProps.sparkMasterUrl, sw);

        AlgBase alg = cmdLineOptionsParser.createAlg(runProps);
        alg.run(sc, sw);
    }


    private ICmdLineOptionsParser<? extends CommonAlgProperties, ? extends AlgBase> findCmdLineOptionsParser(String[] args) {
        if (args.length == 0 || "--help".equals(args[0]) || "-h".equals(args[0])) {
            String algNameExample = algNameToAlgOptionsParser.keySet().iterator().next();
            print(String.format("" +
                            "Usage: algorithm-name options \n\talgorithm-name = %s\n" +
                            "For options try 'algorithm-name --help', e.g.: \n\t%s --help",
                    getSupportedAlgsAsString(), algNameExample));
            return null;
        }

        ICmdLineOptionsParser<? extends CommonAlgProperties, ? extends AlgBase> res = algNameToAlgOptionsParser.get(args[0]);
        if (res == null) {
            String msg = String.format("Unknown algorithm '%s', supported ones are: %s", args[0], getSupportedAlgsAsString());
            throw new IllegalArgumentException(msg);
        }

        return res;
    }

    private String getSupportedAlgsAsString() {
        return StringUtils.join(algNameToAlgOptionsParser.keySet(), " | ");
    }

    private static void print(String str) {
        System.out.println(str);
    }
}
