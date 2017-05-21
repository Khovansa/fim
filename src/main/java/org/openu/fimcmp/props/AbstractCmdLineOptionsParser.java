package org.openu.fimcmp.props;

import org.apache.commons.cli.*;
import org.openu.fimcmp.algbase.AlgBase;
import org.openu.fimcmp.algbase.CommonAlgProperties;

/**
 * Base class for algorithm-specific cmd-line options parsers.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public abstract class AbstractCmdLineOptionsParser<P extends CommonAlgProperties, A extends AlgBase> {
    private static final String HELP_OPTION = "help";

    /**
     * Parses the command-line options.
     *
     * @return either the parsed options or null in case of help request
     */
    public CmdLineOptions<P> parseCmdLine(String[] args) throws ParseException {
        Options options = createCommonOptions();
        addAlgSpecificOptions(options);

        CommandLineParser parser = new BasicParser();
        CommandLine line = parser.parse(options, args);

        if (line.hasOption(HELP_OPTION)) {
            printHelp(options);
            return null;
        }

        return cmdLineToOptions(line);
    }

    public abstract A createAlg(CmdLineOptions<P> cmdLineOptions);


    //Required from subclasses:
    protected abstract void addAlgSpecificOptions(Options options);

    protected abstract P createAlgProperties(CommandLine line, double minSupp);


    //Services for sub-classes:
    protected static int getIntVal(CommandLine line, String opt, int defaultVal) {
        return Integer.parseInt(line.getOptionValue(opt, "" + defaultVal));
    }

    protected static Integer getOptIntVal(CommandLine line, String opt, Integer defaultVal) {
        if (!line.hasOption(opt)) {
            return defaultVal;
        } else {
            return Integer.parseInt(line.getOptionValue(opt, "" + defaultVal));
        }
    }

    protected static double getDoubleVal(CommandLine line, String opt, String defaultVal) {
        return Double.parseDouble(line.getOptionValue(opt, defaultVal));
    }

    protected static boolean getBooleanVal(CommandLine line, String opt, boolean defaultVal) {
        return Boolean.parseBoolean(line.getOptionValue(opt, "" + defaultVal));
    }


    private Options createCommonOptions() {
        Options options = new Options();
        options.addOption("h", "help", false, "Print help and quit");
        options.addOption("url", "spark-master-url", true, "Spark master URL");
        options.addOption("krio", "use-krio", false, "Whether to use krio serialization library");
        options.addOption("input", "input-file-name", true,
                String.format("Either an absolute path or a path relative to '%s' environment variable", CmdLineOptions.INPUT_PATH_ENV_VAR));

        options.addOption("sup", "min-supp", true, "Min support");

        options.addOption("ipnum", "input-parts-num", true, "Number of partitions to read the input file");
        options.addOption("pi", "persist-input", true, "Whether to persist the input RDD");
        options.addOption("print", "print-fks", true, "Whether to print F1, F2, ...");

        return options;
    }

    private void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("ant", options);
    }

    private CmdLineOptions<P> cmdLineToOptions(CommandLine line) {
        String sparkMasterUrl = line.getOptionValue("url", "spark://192.168.1.68:7077");
        boolean isUseKrio = line.hasOption("krio");
        String inputFileName = line.getOptionValue("input");

        double minSupp = getDoubleVal(line, "sup", "0.9");

        P algProps = createAlgProperties(line, minSupp);

        algProps.inputNumParts = getIntVal(line, "ipnum", algProps.inputNumParts);
        algProps.isPersistInput = getBooleanVal(line, "pi", algProps.isPersistInput);
        algProps.isPrintFks = getBooleanVal(line, "print", algProps.isPrintFks);

        return new CmdLineOptions<>(sparkMasterUrl, isUseKrio, inputFileName, algProps);
    }
}
