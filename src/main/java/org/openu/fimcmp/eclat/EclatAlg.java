package org.openu.fimcmp.eclat;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.openu.fimcmp.ItemsetAndTids;
import org.openu.fimcmp.ItemsetAndTidsCollection;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The main class that implements the Eclat algorithm.
 */
public class EclatAlg implements Serializable {
    private final long minSuppCount;

    public EclatAlg(long minSuppCount) {
        this.minSuppCount = minSuppCount;
    }

    public JavaRDD<List<Tuple2<int[], Integer>>> computeFreqItemsetsRdd(
            JavaPairRDD<Integer, ItemsetAndTidsCollection> prefRankAndIsAndTidSetRdd) {
        return
//                prefRankAndIsAndTidSetRdd.mapValues(this::computeFisFromTidSets)
                prefRankAndIsAndTidSetRdd.mapValues(this::asIs)
                .sortByKey()
                .values();
    }

    //TODO - remove this method
    List<Tuple2<int[], Integer>> asIs(ItemsetAndTidsCollection itemsetAndTidsCollection) {
        ArrayList<ItemsetAndTids> itemsetAndTidsList = itemsetAndTidsCollection.getItemsetAndTidsList();
        List<Tuple2<int[], Integer>> res = new ArrayList<>(itemsetAndTidsList.size());
        for (ItemsetAndTids itemsetAndTids : itemsetAndTidsList) {
            res.add(new Tuple2<>(itemsetAndTids.getItemset(), itemsetAndTids.getSupportCnt()));
        }

        return res;
    }

    /**
     * Returns a list of {FI as an array of r1s, its frequency}
     */
    List<Tuple2<int[], Integer>> computeFreqItemsetsSingle(ItemsetAndTidsCollection initFis) {
        //TODO
        ArrayList<ItemsetAndTids> itemsetAndTidsList = initFis.getItemsetAndTidsList();
        if (initFis.isEmpty()) {
            return Collections.emptyList();
        }

        List<Tuple2<int[], Integer>> res = new ArrayList<>(10_000);
        sortByKm1Item(itemsetAndTidsList, initFis.getItemsetSize());
        ItemsetAndTidsCollection nextGen = initFis;
        do {
            nextGen = produceNextGenFis(nextGen);
            res.addAll((nextGen != null) ? nextGen.toResult() : Collections.emptyList());
        } while (nextGen != null && !nextGen.isEmpty());

        return res;
    }

    private void sortByKm1Item(ArrayList<ItemsetAndTids> fis, int itemsetSize) {
        final int sortItemInd = itemsetSize - 1;
        fis.sort((o1, o2) -> Integer.compare(o1.getItem(sortItemInd), o2.getItem(sortItemInd)));
    }

    private ItemsetAndTidsCollection produceNextGenFis(ItemsetAndTidsCollection currGen) {
        ArrayList<ItemsetAndTids> fis = currGen.getItemsetAndTidsList();
        final int kk = currGen.getItemsetSize();
        final int supportCorrection = currGen.getSupportCorrection();

        ArrayList<ItemsetAndTids> resList = new ArrayList<>(10_000);
        for (int ii=0; ii < fis.size() - 1; ++ii) {
            for (int jj = ii + 1; jj < fis.size(); ++jj) {
                ItemsetAndTids is1 = fis.get(ii);
                ItemsetAndTids is2 = fis.get(jj);
                if (!haveCommonPrefix(kk-1, is1, is2)) {
                    break;
                }

                ItemsetAndTids newIs = computeNextSizeItemset(is1, is2);
                if (supportCorrection + newIs.getSupportCnt() >= minSuppCount) {
                    resList.add(newIs);
                }
            }
        }

        Tuple2<ArrayList<ItemsetAndTids>, Integer> res = squeeze(resList, supportCorrection);
        return new ItemsetAndTidsCollection(res._1, res._2, kk+1, currGen.getTotalTids());
    }

    private boolean haveCommonPrefix(int prefSize, ItemsetAndTids is1, ItemsetAndTids is2) {
        //TODO
        return false;
    }

    private ItemsetAndTids computeNextSizeItemset(ItemsetAndTids is1, ItemsetAndTids is2) {
        //TODO
        return null;
    }

    private Tuple2<ArrayList<ItemsetAndTids>, Integer> squeeze(ArrayList<ItemsetAndTids> resList, int supportCorrection) {
        //TODO
        return new Tuple2<>(resList, supportCorrection);
    }
}
