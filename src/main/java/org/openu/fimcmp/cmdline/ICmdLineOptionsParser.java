package org.openu.fimcmp.cmdline;

import org.apache.commons.cli.ParseException;
import org.openu.fimcmp.algs.algbase.AlgBase;
import org.openu.fimcmp.algs.algbase.CommonAlgProperties;

/**
 * Common interface for all algoritm-specific options parsers. <br/>
 * Allows to treat all of them in a uniform way, see {@link CmdLineRunner}.
 */
interface ICmdLineOptionsParser<P extends CommonAlgProperties, A extends AlgBase> {
    /**
     * Parses the command-line options.
     *
     * @return either the parsed options or null in case of help request
     */
    CmdLineOptions<P> parseCmdLine(String[] args, String algName) throws ParseException;

    A createAlg(CmdLineOptions<? extends CommonAlgProperties> cmdLineOptions);
}
