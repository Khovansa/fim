package org.openu.fimcmp.eclat;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.openu.fimcmp.ItemsetAndTids;
import org.openu.fimcmp.ItemsetAndTidsCollection;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * The main class that implements the Eclat algorithm.
 */
public class EclatAlg implements Serializable {
    private final long minSuppCount;

    public EclatAlg(long minSuppCount) {
        this.minSuppCount = minSuppCount;
    }

    public JavaRDD<List<Tuple2<int[], Integer>>> computeFreqItemsets(
            JavaPairRDD<Integer, ItemsetAndTidsCollection> prefRankAndIsAndTidSetRdd) {
        return
//                prefRankAndIsAndTidSetRdd.mapValues(this::computeFisFromTidSets)
                prefRankAndIsAndTidSetRdd.mapValues(this::asIs)
                .sortByKey()
                .values();
    }
    /**
     * Returns a list of {FI as an array of r1s, its frequency}
     */
    List<Tuple2<int[], Integer>> computeFisFromTidSets(ItemsetAndTidsCollection itemsetAndTidsCollection) {
        //TODO
        return null;
    }
    List<Tuple2<int[], Integer>> asIs(ItemsetAndTidsCollection itemsetAndTidsCollection) {
        ArrayList<ItemsetAndTids> itemsetAndTidsList = itemsetAndTidsCollection.getItemsetAndTidsList();
        List<Tuple2<int[], Integer>> res = new ArrayList<>(itemsetAndTidsList.size());
        for (ItemsetAndTids itemsetAndTids : itemsetAndTidsList) {
            res.add(new Tuple2<>(itemsetAndTids.getItemset(), itemsetAndTids.getSupportCnt()));
        }

        return res;
    }
}
