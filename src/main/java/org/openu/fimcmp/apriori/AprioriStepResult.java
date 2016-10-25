package org.openu.fimcmp.apriori;

import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.Collection;

/**
 * Result of a single Apriori step
 */
public class AprioriStepResult<FI> {
    public final JavaRDD<? extends Collection<FI>> trs;
    public JavaRDD<Tuple2<FI, Integer>> fisWithCounts;

    public AprioriStepResult(JavaRDD<? extends Collection<FI>> trs, JavaRDD<Tuple2<FI, Integer>> fisWithCounts) {
        this.trs = trs;
        this.fisWithCounts = fisWithCounts;
    }
}
