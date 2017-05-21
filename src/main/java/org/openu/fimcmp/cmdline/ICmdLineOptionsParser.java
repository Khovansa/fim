package org.openu.fimcmp.cmdline;

import org.apache.commons.cli.ParseException;
import org.openu.fimcmp.algs.algbase.AlgBase;
import org.openu.fimcmp.algs.algbase.CommonAlgProperties;

/**
 * Common interface for all algoritm-specific options parsers. <br/>
 * Allows to treat all of them in a uniform way, see {@link CmdLineRunner}.
 */
public interface ICmdLineOptionsParser<P extends CommonAlgProperties, A extends AlgBase> {
    CmdLineOptions<P> parseCmdLine(String[] args) throws ParseException;

    A createAlg(CmdLineOptions<? extends CommonAlgProperties> cmdLineOptions);
}
