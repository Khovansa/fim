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

    @Override
    public FpGrowthAlg createAlg(CmdLineOptions<? extends CommonAlgProperties> cmdLineOptions) {
        return new FpGrowthAlg((FpGrowthAlgProperties) cmdLineOptions.algProps, cmdLineOptions.getInputFile());
    }

    @Override
    protected void addAlgSpecificOptions(Options options) {
    }

    @Override
    protected FpGrowthAlgProperties createAlgProperties(CommandLine line, double minSupp) {
        return new FpGrowthAlgProperties(minSupp);
    }
}
