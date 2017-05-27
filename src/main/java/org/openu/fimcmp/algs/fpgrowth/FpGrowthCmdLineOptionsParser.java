package org.openu.fimcmp.algs.fpgrowth;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.openu.fimcmp.algs.algbase.CommonAlgProperties;
import org.openu.fimcmp.cmdline.AbstractCmdLineOptionsParser;
import org.openu.fimcmp.cmdline.CmdLineOptions;

/**
 * Parse command line options to create FIN+ algorithm
 */
public class FpGrowthCmdLineOptionsParser extends AbstractCmdLineOptionsParser<FpGrowthAlgProperties, FpGrowthAlg> {
    private static final String PRINT_FIS_OPT = "print-all-fis";
    private static final String CNT_ONLY_OPT = "cnt-only";

    @Override
    public FpGrowthAlg createAlg(CmdLineOptions<? extends CommonAlgProperties> cmdLineOptions) {
        return new FpGrowthAlg((FpGrowthAlgProperties) cmdLineOptions.algProps, cmdLineOptions.getInputFile());
    }

    @Override
    protected void addAlgSpecificOptions(Options options) {
        options.addOption(null, CNT_ONLY_OPT, true,
                "Whether to only count the FIs instead of actually collecting them");

        options.addOption(null, PRINT_FIS_OPT, true, "Whether to print all found frequent itemsets");
    }

    @Override
    protected FpGrowthAlgProperties createAlgProperties(CommandLine line, double minSupp) {
        FpGrowthAlgProperties algProps = new FpGrowthAlgProperties(minSupp);

        algProps.isCountingOnly = getBooleanVal(line, CNT_ONLY_OPT, algProps.isCountingOnly);
        algProps.isPrintAllFis = getBooleanVal(line, PRINT_FIS_OPT, algProps.isPrintAllFis);
        if (algProps.isPrintAllFis && algProps.isCountingOnly) {
            @SuppressWarnings("ConstantConditions")
            String msg = String.format("Contradicting options '--%s %s' and '--%s %s'",
                    CNT_ONLY_OPT, algProps.isCountingOnly, PRINT_FIS_OPT, algProps.isPrintAllFis);
            throw new IllegalArgumentException(msg);
        }

        return algProps;
    }
}
