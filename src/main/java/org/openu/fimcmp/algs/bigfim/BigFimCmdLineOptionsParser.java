package org.openu.fimcmp.algs.bigfim;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.openu.fimcmp.algs.algbase.CommonAlgProperties;
import org.openu.fimcmp.cmdline.AbstractCmdLineOptionsParser;
import org.openu.fimcmp.cmdline.CmdLineOptions;

/**
 * Parse command line options to create BigFim algorithm
 */
public class BigFimCmdLineOptionsParser extends AbstractCmdLineOptionsParser<BigFimAlgProperties, BigFimAlg> {

    private static final String ECLAT_PREF_LEN_OPT = "start-eclat-prefix-len";
    private static final String CURR_TO_PREV_THR_OPT = "curr-to-prev-res-ratio-threshold";
    private static final String ECLAT_DIFF_SETS_OPT = "eclat-use-diff-sets";
    private static final String ECLAT_SQUEEZE_OPT = "eclat-use-squeezing";
    private static final String ECLAT_PARTS_NUM_OPT = "eclat-parts-num";

    @Override
    public BigFimAlg createAlg(CmdLineOptions<? extends CommonAlgProperties> cmdLineOptions) {
        return new BigFimAlg((BigFimAlgProperties) cmdLineOptions.algProps, cmdLineOptions.getInputFile());
    }

    @Override
    protected void addAlgSpecificOptions(Options options) {
        options.addOption(null, ECLAT_PREF_LEN_OPT, true, "Prefix length to switch from Apriori to Eclat");

        options.addOption(null, CURR_TO_PREV_THR_OPT, true,
                "Threshold to determine sparse datasets for which continue with Apriori");

        options.addOption(null, ECLAT_DIFF_SETS_OPT, true, "Eclat: whether to enable the diff-sets");
        options.addOption(null, ECLAT_SQUEEZE_OPT, true, "Eclat: whether to enable the squeezing");
        options.addOption(null, ECLAT_PARTS_NUM_OPT, true, "Number of partitions to use for Eclat");
    }

    @Override
    protected BigFimAlgProperties createAlgProperties(CommandLine line, double minSupp) {
        int prefixLenToStartEclat = getIntVal(line, ECLAT_PREF_LEN_OPT, 3);
        BigFimAlgProperties algProps = new BigFimAlgProperties(minSupp, prefixLenToStartEclat);

        algProps.currToPrevResSignificantIncreaseRatio =
                getDoubleVal(line, CURR_TO_PREV_THR_OPT, "" + algProps.currToPrevResSignificantIncreaseRatio);

        algProps.isUseDiffSets = getBooleanVal(line, ECLAT_DIFF_SETS_OPT, algProps.isUseDiffSets);
        algProps.isSqueezingEnabled = getBooleanVal(line, ECLAT_SQUEEZE_OPT, algProps.isSqueezingEnabled);
        algProps.maxEclatNumParts = getOptIntVal(line, ECLAT_PARTS_NUM_OPT, algProps.maxEclatNumParts);

        return algProps;
    }

    @Override
    protected String getCntOnlyOptionName() {
        return "eclat-cnt-only";
    }
}
