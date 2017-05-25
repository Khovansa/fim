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

    @Override
    public FpGrowthAlg createAlg(CmdLineOptions<? extends CommonAlgProperties> cmdLineOptions) {
        return new FpGrowthAlg((FpGrowthAlgProperties) cmdLineOptions.algProps, cmdLineOptions.getInputFile());
    }

    @Override
    protected void addAlgSpecificOptions(Options options) {
        options.addOption(null, PRINT_FIS_OPT, true, "Whether to print all found frequent itemsets");
    }

    @Override
    protected FpGrowthAlgProperties createAlgProperties(CommandLine line, double minSupp) {
        FpGrowthAlgProperties algProps = new FpGrowthAlgProperties(minSupp);

        algProps.isPrintAllFis = getBooleanVal(line, PRINT_FIS_OPT, algProps.isPrintAllFis);

        return algProps;
    }
}
