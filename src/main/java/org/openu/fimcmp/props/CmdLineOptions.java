package org.openu.fimcmp.props;

import org.openu.fimcmp.algbase.CommonAlgProperties;

/**
 * Holds the command line options to run a specific algorithm
 */
public class CmdLineOptions<P extends CommonAlgProperties> {
    public final String sparkMasterUrl;
    public final boolean isUseKrio;
    public final String inputFileName;
    public final P algProps;

    public CmdLineOptions(String sparkMasterUrl, boolean isUseKrio, String inputFileName, P algProps) {
        this.sparkMasterUrl = sparkMasterUrl;
        this.isUseKrio = isUseKrio;
        this.inputFileName = inputFileName;
        this.algProps = algProps;
    }
}
