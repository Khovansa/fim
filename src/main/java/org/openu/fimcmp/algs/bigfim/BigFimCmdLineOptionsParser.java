package org.openu.fimcmp.algs.bigfim;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.openu.fimcmp.cmdline.AbstractCmdLineOptionsParser;
import org.openu.fimcmp.cmdline.CmdLineOptions;

/**
 * Parse command line options to create BigFim algorithm
 */
public class BigFimCmdLineOptionsParser extends AbstractCmdLineOptionsParser<BigFimAlgProperties, BigFimAlg> {
    @Override
    public BigFimAlg createAlg(CmdLineOptions<BigFimAlgProperties> cmdLineOptions) {
        return new BigFimAlg(cmdLineOptions.algProps, cmdLineOptions.getInputFile());
    }

    @Override
    protected void addAlgSpecificOptions(Options options) {
        options.addOption("eplen", "start-eclat-prefix-len", true, "Prefix length to switch from Apriori to Eclat");

        options.addOption("sratio", "curr-to-prev-res-ratio-threshold", true,
                "Threshold to determine sparse datasets for which continue with Apriori");

        options.addOption("ediff", "eclat-use-diff-sets", true, "Eclat: whether to enable the diff-sets");
        options.addOption("esqueeze", "eclat-use-squeezing", true, "Eclat: whether to enable the squeezing");
        options.addOption("ecnt", "eclat-cnt-only", true, "Whether to only count the results in Eclat");
        options.addOption("epnum", "eclat-parts-num", true, "Number of partitions to use for Eclat");
    }

    @Override
    protected BigFimAlgProperties createAlgProperties(CommandLine line, double minSupp) {
        int prefixLenToStartEclat = getIntVal(line, "eplen", 3);
        BigFimAlgProperties algProps = new BigFimAlgProperties(minSupp, prefixLenToStartEclat);

        algProps.currToPrevResSignificantIncreaseRatio =
                getDoubleVal(line, "sratio", "" + algProps.currToPrevResSignificantIncreaseRatio);

        algProps.isUseDiffSets = getBooleanVal(line, "ediff", algProps.isUseDiffSets);
        algProps.isSqueezingEnabled = getBooleanVal(line, "esqueeze", algProps.isSqueezingEnabled);
        algProps.isCountingOnly = getBooleanVal(line, "ecnt", algProps.isCountingOnly);
        algProps.maxEclatNumParts = getOptIntVal(line, "epnum", algProps.maxEclatNumParts);

        return algProps;
    }
}
