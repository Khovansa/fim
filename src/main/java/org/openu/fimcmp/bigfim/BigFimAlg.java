package org.openu.fimcmp.bigfim;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.openu.fimcmp.BasicOps;

import java.io.Serializable;

/**
 * The main class that implements the Big FIM algorithm.
 */
public class BigFimAlg implements Serializable {
    private final BigFimProperties props;

    public BigFimAlg(BigFimProperties props) {
        this.props = props;
    }

    public JavaRDD<String[]> readInput(JavaSparkContext sc, String inputFile) {
        return BasicOps.readLinesAsSortedItemsArr(inputFile, props.inputNumParts, sc);
    }

    public BigFimResult computeFis(JavaRDD<String[]> trs) {
        //TODO
        return null;
    }
}
