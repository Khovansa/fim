package org.openu.fimcmp.cmdline;

import org.openu.fimcmp.algs.algbase.AlgBase;
import org.openu.fimcmp.algs.algbase.CommonAlgProperties;
import org.openu.fimcmp.algs.bigfim.BigFimCmdLineOptionsParser;
import org.openu.fimcmp.algs.fin.FinCmdLineOptionsParser;

import java.util.Map;
import java.util.TreeMap;

/**
 * The main class of the 'fim-cmp' project and the main entry point. <br/>
 * Parses the command-line options and dispatches the execution to the appropriate algorithm.
 */
@SuppressWarnings("WeakerAccess")
public class CmdLineRunner {
    private final Map<String, ICmdLineOptionsParser<? extends CommonAlgProperties, ? extends AlgBase>> algNameToAlgOptionsParser;

    public static void main(String[] args) throws Exception {
        CmdLineRunner runner = new CmdLineRunner();
        runner.run(args);
    }

    public CmdLineRunner() {
        this(defaultAlgNameToAlgOptionsParser());
    }

    public CmdLineRunner(Map<String, ICmdLineOptionsParser<? extends CommonAlgProperties, ? extends AlgBase>> algNameToAlgOptionsParser) {
        this.algNameToAlgOptionsParser = algNameToAlgOptionsParser;
    }

    public void run(String[] args) throws Exception {
        //TODO
        ICmdLineOptionsParser<? extends CommonAlgProperties, ? extends AlgBase> cmdLineOptionsParser =
                algNameToAlgOptionsParser.get(args[0]);
        CmdLineOptions<? extends CommonAlgProperties> runProps = cmdLineOptionsParser.parseCmdLine(args);
        AlgBase alg = cmdLineOptionsParser.createAlg(runProps);
        alg.run(null, null);
    }

    private static Map<String, ICmdLineOptionsParser<? extends CommonAlgProperties, ? extends AlgBase>> defaultAlgNameToAlgOptionsParser() {
        Map<String, ICmdLineOptionsParser<? extends CommonAlgProperties, ? extends AlgBase>> res = new TreeMap<>();
        res.put("BIG_FIM", new BigFimCmdLineOptionsParser());
        res.put("FIN", new FinCmdLineOptionsParser());
        return res;
    }
}
