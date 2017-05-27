package org.openu.fimcmp.algs.fin;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.openu.fimcmp.algs.algbase.CommonAlgProperties;
import org.openu.fimcmp.cmdline.AbstractCmdLineOptionsParser;
import org.openu.fimcmp.cmdline.CmdLineOptions;

/**
 * Parse command line options to create FIN+ algorithm
 */
public class FinCmdLineOptionsParser extends AbstractCmdLineOptionsParser<FinAlgProperties, FinAlg> {
    private static final String RUN_TYPE_ALLOWED_VALUES = StringUtils.join(FinAlgProperties.RunType.values(), " | ");
    private static final String RUN_TYPE_OPT = "run-type";
    private static final String ITEMSET_SEQ_LEN_OPT = "itemset-len-for-seq-processing";

    @Override
    public FinAlg createAlg(CmdLineOptions<? extends CommonAlgProperties> cmdLineOptions) {
        return new FinAlg((FinAlgProperties)cmdLineOptions.algProps, cmdLineOptions.getInputFile());
    }

    @Override
    protected void addAlgSpecificOptions(Options options) {
        options.addOption(null, RUN_TYPE_OPT, true, RUN_TYPE_ALLOWED_VALUES);

        options.addOption(null, ITEMSET_SEQ_LEN_OPT, true,
                "The required itemset length of the nodes processed sequentially on the driver machine, e.g. '1' for items");
    }

    @Override
    protected FinAlgProperties createAlgProperties(CommandLine line, double minSupp) {
        FinAlgProperties algProps = new FinAlgProperties(minSupp);

        String runTypeStr = line.getOptionValue(RUN_TYPE_OPT);
        try {
            algProps.runType = FinAlgProperties.RunType.valueOf(runTypeStr);
        } catch (RuntimeException ex) {
            throw new IllegalArgumentException(errMsg(RUN_TYPE_OPT, runTypeStr, RUN_TYPE_ALLOWED_VALUES));
        }

        algProps.requiredItemsetLenForSeqProcessing = getIntVal(
                line, ITEMSET_SEQ_LEN_OPT, algProps.requiredItemsetLenForSeqProcessing);

        return algProps;
    }
}
