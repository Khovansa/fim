package org.openu.fimcmp.props;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.openu.fimcmp.algbase.CommonAlgProperties;

import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Holds the command line options to run a specific algorithm
 */
public class CmdLineOptions<P extends CommonAlgProperties> {
    //C:\Users\Alexander\Desktop\Data Mining\DataSets
    static final String INPUT_PATH_ENV_VAR = "FIM_CMP_INPUT_PATH";

    public final String sparkMasterUrl;
    public final boolean isUseKrio;
    private final String inputFile;
    public final P algProps;

    @SuppressWarnings("WeakerAccess")
    public CmdLineOptions(String sparkMasterUrl, boolean isUseKrio, String inputFileName, P algProps) {
        this.sparkMasterUrl = sparkMasterUrl;
        this.isUseKrio = isUseKrio;
        this.inputFile = findInputFile(inputFileName);
        this.algProps = algProps;
    }

    public String getInputFile() {
        return inputFile;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.MULTI_LINE_STYLE);
    }

    private static String findInputFile(String inputFileName) {
        String inputFile;
        if (Paths.get(inputFileName).isAbsolute()) {
            inputFile = inputFileName;
        } else {
            String inputPath = System.getenv(INPUT_PATH_ENV_VAR);
            if (inputPath == null) {
                String msg = String.format("Input file name '%s' is relative and environment variable '%s' is not defined",
                        inputFileName, INPUT_PATH_ENV_VAR);
                throw new IllegalArgumentException(msg);
            }
            inputFile = Paths.get(inputPath, inputFileName).toString();
        }

        if (!Files.isReadable(Paths.get(inputFile))) {
            String msg = String.format("Input file '%s' does not exist or is not readable", inputFileName);
            throw new IllegalArgumentException(msg);
        }

        return inputFile;
    }
}
