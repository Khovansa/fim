package org.openu.fimcmp;

import org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 *
 */
public class TestDataLocation {
    private static final Path TEST_DATA_DIR = Paths.get("C:\\Users\\Alexander\\Desktop\\Data Mining\\DataSets");

    public static Path file(String fileName) {
        Path res = TEST_DATA_DIR.resolve(fileName);
        if (!res.toFile().exists()) {
            throw new RuntimeException(String.format("File '%s' does not exist", res));
        }
        return res;
    }

    public static String fileStr(String fileName) {
        return file(fileName).toString();
    }

    public static String outDir(String dir) {
        try {
            FileUtils.deleteDirectory(Paths.get(dir).toFile());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return dir;
    }
}
