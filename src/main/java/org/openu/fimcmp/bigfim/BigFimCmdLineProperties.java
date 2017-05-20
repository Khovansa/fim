package org.openu.fimcmp.bigfim;

import org.apache.commons.cli.*;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * Parse and hold the command-line options for the BigFimAlg.
 */
class BigFimCmdLineProperties {
    final String sparkMasterUrl;
    final boolean isUseKrio;
    final String inputFileName;
    final BigFimAlgProperties bigFimAlgProps;

    static BigFimCmdLineProperties parse(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("h", "help", false, "Print help and quit");
        options.addOption("url", "spark-master-url", true, "Spark master URL");
        options.addOption("krio", "use-krio", false, "Whether to use krio serialization library");
        options.addOption("input", "input-file-name", true, "Input file name (not full path)");

        options.addOption("sup", "min-supp", true, "Min support");
        options.addOption("eplen", "start-eclat-prefix-len", true, "Prefix length to switch from Apriori to Eclat");

        options.addOption("ipnum", "input-parts-num", true, "Number of partitions to read the input file");
        options.addOption("pi", "persist-input", true, "Whether to persist the input RDD");
        options.addOption("print", "print-fks", true, "Whether to print F1, F2, ...");
        options.addOption("sratio", "curr-to-prev-res-ratio-threshold", true,
                "Threshold to determine sparse datasets for which continue with Apriori");

        options.addOption("ediff", "eclat-use-diff-sets", true, "Eclat: whether to enable the diff-sets");
        options.addOption("esqueeze", "eclat-use-squeezing", true, "Eclat: whether to enable the squeezing");
        options.addOption("ecnt", "eclat-cnt-only", true, "Whether to only count the results in Eclat");
        options.addOption("epnum", "eclat-parts-num", true, "Number of partitions to use for Eclat");

        CommandLineParser parser = new BasicParser();
        CommandLine line = parser.parse( options, args);
        if (line.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("ant", options);
            return null;
        }

        return cmdOptionsToRunProps(line);
    }

    @SuppressWarnings("WeakerAccess")
    BigFimCmdLineProperties(
            String sparkMasterUrl, boolean isUseKrio, String inputFileName, BigFimAlgProperties bigFimAlgProps) {
        this.sparkMasterUrl = sparkMasterUrl;
        this.isUseKrio = isUseKrio;
        this.inputFileName = inputFileName;
        this.bigFimAlgProps = bigFimAlgProps;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.MULTI_LINE_STYLE);
    }

    private static BigFimCmdLineProperties cmdOptionsToRunProps(CommandLine line) {
        String sparkMasterUrl = line.getOptionValue("url", "spark://192.168.1.68:7077");
        boolean isUseKrio = line.hasOption("krio");
        String inputFileName = line.getOptionValue("input");

        double minSupp = getDoubleVal(line, "sup", "0.9");
        int prefixLenToStartEclat = getIntVal(line, "eplen", 3);
        BigFimAlgProperties algProps = new BigFimAlgProperties(minSupp, prefixLenToStartEclat);

        algProps.inputNumParts = getIntVal(line, "ipnum", algProps.inputNumParts);
        algProps.isPersistInput = getBooleanVal(line, "pi", algProps.isPersistInput);
        algProps.isPrintFks = getBooleanVal(line, "print", algProps.isPrintFks);
        algProps.currToPrevResSignificantIncreaseRatio =
                getDoubleVal(line, "sratio", "" + algProps.currToPrevResSignificantIncreaseRatio);

        algProps.isUseDiffSets = getBooleanVal(line, "ediff", algProps.isUseDiffSets);
        algProps.isSqueezingEnabled = getBooleanVal(line, "esqueeze", algProps.isSqueezingEnabled);
        algProps.isCountingOnly = getBooleanVal(line, "ecnt", algProps.isCountingOnly);
        algProps.maxEclatNumParts = getOptIntVal(line, "epnum", algProps.maxEclatNumParts);

        return new BigFimCmdLineProperties(sparkMasterUrl, isUseKrio, inputFileName, algProps);
    }

    private static int getIntVal(CommandLine line, String opt, int defaultVal) {
        return Integer.parseInt(line.getOptionValue(opt, "" + defaultVal));
    }

    private static Integer getOptIntVal(CommandLine line, String opt, Integer defaultVal) {
        if (!line.hasOption(opt)) {
            return defaultVal;
        } else {
            return Integer.parseInt(line.getOptionValue(opt, "" + defaultVal));
        }
    }

    private static double getDoubleVal(CommandLine line, String opt, String defaultVal) {
        return Double.parseDouble(line.getOptionValue(opt, defaultVal));
    }

    private static boolean getBooleanVal(CommandLine line, String opt, boolean defaultVal) {
        return Boolean.parseBoolean(line.getOptionValue(opt, "" + defaultVal));
    }
}
