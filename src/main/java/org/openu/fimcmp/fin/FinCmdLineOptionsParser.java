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

    @Override
    public FinAlg createAlg(CmdLineOptions<FinAlgProperties> cmdLineOptions) {
        return new FinAlg(cmdLineOptions.algProps, cmdLineOptions.getInputFile());
    }

    @Override
    protected void addAlgSpecificOptions(Options options) {
        options.addOption("rtype", "run-type", true, RUN_TYPE_ALLOWED_VALUES);

        options.addOption("isdlen", "itemset-len-to-process-on-driver", true,
                "The required itemset length of the nodes processed sequentially on the driver machine, e.g. '1' for items");
    }

    @Override
    protected FinAlgProperties createAlgProperties(CommandLine line, double minSupp) {
        FinAlgProperties algProps = new FinAlgProperties(minSupp);

        String runTypeStr = line.getOptionValue("rtype");
        try {
            algProps.runType = FinAlgProperties.RunType.valueOf(runTypeStr);
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException(
                    String.format("Bad run type '%s', allowed values: %s", runTypeStr, RUN_TYPE_ALLOWED_VALUES));
        }

        algProps.requiredItemsetLenForSeqProcessing = getIntVal(line, "isdlen", algProps.requiredItemsetLenForSeqProcessing);

        return algProps;
    }
}
