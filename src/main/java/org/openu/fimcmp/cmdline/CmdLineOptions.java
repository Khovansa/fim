package org.openu.fimcmp.cmdline;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.openu.fimcmp.algs.algbase.CommonAlgProperties;

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
    final int sleepSecs;

    @SuppressWarnings("WeakerAccess")
    public CmdLineOptions(String sparkMasterUrl, boolean isUseKrio, String inputFileName, P algProps, int sleepSecs) {
        this.sparkMasterUrl = sparkMasterUrl;
        this.isUseKrio = isUseKrio;
        this.inputFile = findInputFile(inputFileName);
        this.algProps = algProps;
        this.sleepSecs = sleepSecs;
    }

    public String getInputFile() {
        return inputFile;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.MULTI_LINE_STYLE);
    }

    private static String findInputFile(String inputFileName) {
        if (inputFileName.contains("://")) {
            return inputFileName; //protocols such as s3:// or hdfs:// - no standard checks, relying on Spark to access them
        }

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
