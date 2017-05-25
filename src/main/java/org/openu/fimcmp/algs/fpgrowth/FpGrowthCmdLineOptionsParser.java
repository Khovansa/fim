package org.openu.fimcmp.algs.fpgrowth;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.openu.fimcmp.algs.algbase.CommonAlgProperties;
import org.openu.fimcmp.algs.fin.FinAlg;
import org.openu.fimcmp.algs.fin.FinAlgProperties;
import org.openu.fimcmp.cmdline.AbstractCmdLineOptionsParser;
import org.openu.fimcmp.cmdline.CmdLineOptions;

/**
 * Parse command line options to create FIN+ algorithm
 */
public class FpGrowthCmdLineOptionsParser extends AbstractCmdLineOptionsParser<FinAlgProperties, FinAlg> {
    private static final String PRINT_FIS_OPT = "print-all-fis";

    @Override
    public FinAlg createAlg(CmdLineOptions<? extends CommonAlgProperties> cmdLineOptions) {
        return new FinAlg((FinAlgProperties)cmdLineOptions.algProps, cmdLineOptions.getInputFile());
    }

    @Override
    protected void addAlgSpecificOptions(Options options) {
        options.addOption(null, PRINT_FIS_OPT, true, "Whether to print all found frequent itemsets");
    }

    @Override
    protected FinAlgProperties createAlgProperties(CommandLine line, double minSupp) {
        FinAlgProperties algProps = new FinAlgProperties(minSupp);

        algProps.isPrintAllFis = getBooleanVal(line, PRINT_FIS_OPT, algProps.isPrintAllFis);

        return algProps;
    }
}
