package org.openu.fimcmp.fin;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.openu.fimcmp.props.AbstractCmdLineOptionsParser;
import org.openu.fimcmp.props.CmdLineOptions;

/**
 * Parse command line options to create FIN+ algorithm
 */
public class FinCmdLineOptionsParser extends AbstractCmdLineOptionsParser<FinAlgProperties, FinAlg> {
    private static final String RUN_TYPE_ALLOWED_VALUES = StringUtils.join(FinAlgProperties.RunType.values(), "|");
    private static final String RUN_TYPE_SHORT_OPT = "rtype";
    private static final String IS_SEQ_LEN_SHORT_OPT = "islenseq";
    private static final String CNT_ONLY_SHORT_OPT = "cnt";
    private static final String PRINT_FIS_SHORT_OPT = "printall";

    @Override
    public FinAlg createAlg(CmdLineOptions<FinAlgProperties> cmdLineOptions) {
        return new FinAlg(cmdLineOptions.algProps, cmdLineOptions.getInputFile());
    }

    @Override
    protected void addAlgSpecificOptions(Options options) {
        options.addOption(RUN_TYPE_SHORT_OPT, "run-type", true, RUN_TYPE_ALLOWED_VALUES);

        options.addOption(IS_SEQ_LEN_SHORT_OPT, "itemset-len-for-seq-processing", true,
                "The required itemset length of the nodes processed sequentially on the driver machine, e.g. '1' for items");

        options.addOption(CNT_ONLY_SHORT_OPT, "cnt-only", true,
                "Whether to only count the FIs instead of actually collecting them");

        options.addOption(PRINT_FIS_SHORT_OPT, "print-all-fis", true, "Whether to print all found frequent itemsets");
    }

    @Override
    protected FinAlgProperties createAlgProperties(CommandLine line, double minSupp) {
        FinAlgProperties algProps = new FinAlgProperties(minSupp);

        String runTypeStr = line.getOptionValue(RUN_TYPE_SHORT_OPT);
        try {
            algProps.runType = FinAlgProperties.RunType.valueOf(runTypeStr);
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException(
                    String.format("Bad run type '%s', allowed values: %s", runTypeStr, RUN_TYPE_ALLOWED_VALUES));
        }

        algProps.requiredItemsetLenForSeqProcessing = getIntVal(
                line, IS_SEQ_LEN_SHORT_OPT, algProps.requiredItemsetLenForSeqProcessing);

        algProps.isCountingOnly = getBooleanVal(line, CNT_ONLY_SHORT_OPT, algProps.isCountingOnly);
        algProps.isPrintAllFis = getBooleanVal(line, PRINT_FIS_SHORT_OPT, algProps.isPrintAllFis);
        if (algProps.isPrintAllFis && algProps.isCountingOnly) {
            throw new IllegalArgumentException("Contradicting options: can't print all FIs if we only count them");
        }

        return algProps;
    }
}
