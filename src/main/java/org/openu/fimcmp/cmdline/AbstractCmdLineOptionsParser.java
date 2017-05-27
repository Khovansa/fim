package org.openu.fimcmp.cmdline;

import org.apache.commons.cli.*;
import org.apache.commons.lang3.StringUtils;
import org.openu.fimcmp.algs.algbase.AlgBase;
import org.openu.fimcmp.algs.algbase.CommonAlgProperties;

/**
 * Base class for algorithm-specific cmd-line options parsers.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public abstract class AbstractCmdLineOptionsParser<P extends CommonAlgProperties, A extends AlgBase>
        implements ICmdLineOptionsParser<P, A> {
    private static final String HELP_OPT = "help";
    private static final String MASTER_URL_OPT = "spark-master-url";
    private static final String USE_KRIO_OPT = "use-krio";
    private static final String INPUT_FILE_OPT = "input-file-name";
    private static final String MIN_SUPP_OPT = "min-supp";
    private static final String INPUT_PARTS_NUM_OPT = "input-parts-num";
    private static final String PERSIST_INPUT_OPT = "persist-input";
    private static final String PRINT_PART_OPT = "print-intermediate-res";
    private static final String PRINT_FIS_OPT = "print-all-fis";

    @Override
    public CmdLineOptions<P> parseCmdLine(String[] args) throws ParseException {
        Options options = createCommonOptions();
        addAlgSpecificOptions(options);

        CommandLineParser parser = new BasicParser();
        CommandLine line = parser.parse(options, args);

        if (line.hasOption(HELP_OPT)) {
            printHelp(options);
            return null;
        }

        return cmdLineToOptions(line);
    }


    //Required from subclasses:
    protected abstract void addAlgSpecificOptions(Options options);

    protected abstract P createAlgProperties(CommandLine line, double minSupp);

    protected String getCntOnlyOptionName() {
        return "cnt-only";
    }

    //Services for sub-classes:
    protected static int getIntVal(CommandLine line, String opt, int defaultVal) {
        String val = line.getOptionValue(opt, "" + defaultVal);
        try {
            return Integer.parseInt(val);
        } catch (RuntimeException ex) {
            throw new IllegalArgumentException(errMsg(opt, val, "<any integer number>"));
        }
    }

    protected static Integer getOptIntVal(CommandLine line, String opt, Integer defaultVal) {
        if (!line.hasOption(opt)) {
            return defaultVal;
        }

        String val = line.getOptionValue(opt, "" + defaultVal);
        try {
            return Integer.parseInt(val);
        } catch (RuntimeException ex) {
            throw new IllegalArgumentException(errMsg(opt, val, "<any integer number>"));
        }
    }

    protected static double getDoubleVal(CommandLine line, String opt, String defaultVal) {
        String val = line.getOptionValue(opt, defaultVal);
        try {
            return Double.parseDouble(val);
        } catch (RuntimeException ex) {
            throw new IllegalArgumentException(errMsg(opt, val, "<any floating-point number>"));
        }
    }

    protected static boolean getBooleanVal(CommandLine line, String opt, boolean defaultVal) {
        String val = line.getOptionValue(opt, "" + defaultVal);
        try {
            return Boolean.parseBoolean(val);
        } catch (RuntimeException ex) {
            throw new IllegalArgumentException(errMsg(opt, val, "false | true"));
        }
    }

    protected static String errMsg(String opt, String val, String allowedValues) {
        if (StringUtils.isBlank(val)) {
            return String.format("Missing mandatory option '--%s', allowed values: %s", opt, allowedValues);
        } else {
            return String.format("Bad value of '--%s %s', allowed values: %s", opt, val, allowedValues);
        }
    }

    private Options createCommonOptions() {
        Options options = new Options();
        options.addOption(null, HELP_OPT, false, "Print help and quit");
        options.addOption(null, MASTER_URL_OPT, true, "Spark master URL");
        options.addOption(null, USE_KRIO_OPT, false, "Whether to use krio serialization library");
        options.addOption(null, INPUT_FILE_OPT, true,
                String.format("Either an absolute path or a path relative to '%s' environment variable", CmdLineOptions.INPUT_PATH_ENV_VAR));

        options.addOption(null, MIN_SUPP_OPT, true, "Min support");

        options.addOption(null, INPUT_PARTS_NUM_OPT, true, "Number of partitions to read the input file");
        options.addOption(null, PERSIST_INPUT_OPT, true, "Whether to persist the input RDD");

        options.addOption(null, PRINT_PART_OPT, true,
                "Whether to print F1, F2, ..., and also some progress info");
        options.addOption(null, getCntOnlyOptionName(), true,
                "Whether to only count the FIs instead of actually collecting them");
        options.addOption(null, PRINT_FIS_OPT, true, "Whether to print all found frequent itemsets");

        return options;
    }

    private void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("ant", options);
    }

    private CmdLineOptions<P> cmdLineToOptions(CommandLine line) {
        String sparkMasterUrl = line.getOptionValue(MASTER_URL_OPT, "spark://192.168.1.68:7077");
        boolean isUseKrio = line.hasOption(USE_KRIO_OPT);
        String inputFileName = line.getOptionValue(INPUT_FILE_OPT);

        double minSupp = getDoubleVal(line, MIN_SUPP_OPT, "0.9");

        P algProps = createAlgProperties(line, minSupp);

        algProps.inputNumParts = getIntVal(line, INPUT_PARTS_NUM_OPT, algProps.inputNumParts);
        algProps.isPersistInput = getBooleanVal(line, PERSIST_INPUT_OPT, algProps.isPersistInput);

        algProps.isPrintIntermediateRes = getBooleanVal(line, PRINT_PART_OPT, algProps.isPrintIntermediateRes);
        final String cntOnlyOptionName = getCntOnlyOptionName();
        algProps.isCountingOnly = getBooleanVal(line, cntOnlyOptionName, algProps.isCountingOnly);
        algProps.isPrintAllFis = getBooleanVal(line, PRINT_FIS_OPT, algProps.isPrintAllFis);
        if (algProps.isPrintAllFis && algProps.isCountingOnly) {
            @SuppressWarnings("ConstantConditions")
            String msg = String.format("Contradicting options '--%s %s' and '--%s %s'",
                    cntOnlyOptionName, algProps.isCountingOnly, PRINT_FIS_OPT, algProps.isPrintAllFis);
            throw new IllegalArgumentException(msg);
        }

        return new CmdLineOptions<>(sparkMasterUrl, isUseKrio, inputFileName, algProps);
    }
}
