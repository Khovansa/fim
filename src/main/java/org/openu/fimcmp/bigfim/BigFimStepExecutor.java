package org.openu.fimcmp.bigfim;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import org.openu.fimcmp.ItemsetAndTidsCollection;
import org.openu.fimcmp.apriori.*;
import org.openu.fimcmp.eclat.EclatAlg;
import org.openu.fimcmp.eclat.EclatProperties;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Executes steps of BigFIM algorithm. <br/>
 * Unlike the upper-level BigFimAlg, holds state that allows it to move from one step to the other. <br/>
 */
class BigFimStepExecutor {
    private final BigFimProperties props;
    private final AprContext cxt;
    private final ArrayList<List<long[]>> aprioriFis;
    private final ArrayList<JavaRDD> allRanksRdds;

    BigFimStepExecutor(BigFimProperties props, AprContext cxt) {
        this.props = props;
        this.cxt = cxt;
        aprioriFis = new ArrayList<>();
        allRanksRdds = new ArrayList<>();

        aprioriFis.add(cxt.freqItemRanksAsItemsetBs());
    }

    boolean isContinueWithApriori() {
        if (!canContinue()) {
            return false;
        }

        final int currPrefLen = aprioriFis.size() - 1;
        if (currPrefLen < props.prefixLenToStartEclat) {
            //if we have not reached the possible 'Eclat switching point', just continue
            return true;
        }

        //We have reached the possible 'Eclat switching point', now need to decide whether indeed to switch to Eclat:
        if (props.prefixLenToStartEclat <= 1) {
            //prefix length = 1 is a special case: if chosen, never go beyond it
            return false;
        }

        //the general case: check whether Apriori could produce the results fast - this happens in sparse datasets:
        assert aprioriFis.size() >= 2 : "size > currPrefLen >= prefixLenToStartEclat >= 2";
        List<long[]> lastRes = aprioriFis.get(aprioriFis.size() - 1);
        List<long[]> prevRes = aprioriFis.get(aprioriFis.size() - 2);
        double resIncreaseRatio = (1.0 * lastRes.size()) / prevRes.size();
        boolean isSignificantlyIncreased = (resIncreaseRatio < props.currToPrevResSignificantIncreaseRatio);
        //if the increase is not significant, it is a sparse dataset and we should continue with Apriori:
        return !isSignificantlyIncreased;
    }

    boolean canContinue() {
        List<long[]> lastRes = aprioriFis.get(aprioriFis.size() - 1);
        return lastRes.size() > 1;
    }

    JavaRDD<int[]> computeRddRanks1(JavaRDD<String[]> trs) {
        JavaRDD<int[]> res = cxt.computeRddRanks1(trs);
        allRanksRdds.add(res);
        return res;
    }

    JavaRDD<Tuple2<int[], long[]>> computeCurrSizeRdd(
            AprioriStepRes currStep, JavaRDD<Tuple2<int[], long[]>> ranks1AndKm1, JavaRDD<int[]> ranks1Rdd,
            boolean isForEclat) {
        JavaRDD<Tuple2<int[], long[]>> res;
        StorageLevel storageLevel;
        if (ranks1AndKm1 == null) {
            res = cxt.apr.toRddOfRanks1And2(ranks1Rdd, currStep.currSizeRanks);
            storageLevel = StorageLevel.MEMORY_ONLY_SER();
        } else {
            res = cxt.apr.toRddOfRanks1AndK(ranks1AndKm1, currStep.currSizeRanks);
            storageLevel = StorageLevel.MEMORY_AND_DISK_SER();
        }

        if (!isForEclat) {
            res = res.persist(storageLevel);
            unpersistPrevIfNeeded();
            cxt.pp("Computed and saved RDD ranks " + currStep.kk);
        }

        allRanksRdds.add(res);
        return res;
    }
    AprioriStepRes computeF2(JavaRDD<int[]> ranks1Rdd) {
        List<int[]> fkAsArrays = cxt.apr.computeF2_Part(ranks1Rdd, cxt.totalFreqItems);

        FiRanksToFromItems prevSizeAllRanks = new FiRanksToFromItems();
        AprioriStepRes res = new AprioriStepRes(2, fkAsArrays, prevSizeAllRanks, cxt.totalFreqItems, cxt);

        res.print(cxt, props.isPrintFks);

        aprioriFis.add(res.getItemsetBitsets(cxt));
        return res;
    }

    AprioriStepRes computeFk(JavaRDD<Tuple2<int[], long[]>> ranks1AndK, AprioriStepRes currStep) {
        NextSizeItemsetGenHelper nextSizeGenHelper = currStep.computeNextSizeGenHelper(cxt.totalFreqItems);
        final int kp1 = currStep.kk + 1;
        List<int[]> fkAsArrays = cxt.apr.computeFk_Part(kp1, ranks1AndK, nextSizeGenHelper);

        FiRanksToFromItems prevSizeAllRanks = currStep.currSizeAllRanks;
        AprioriStepRes res = new AprioriStepRes(kp1, fkAsArrays, prevSizeAllRanks, currStep.getFkSize(), cxt);

        res.print(cxt, props.isPrintFks);

        aprioriFis.add(res.getItemsetBitsets(cxt));
        return res;
    }

    JavaRDD<List<long[]>> computeWithEclat(AprioriStepRes currStep, JavaRDD<Tuple2<int[], long[]>> ranks1AndK) {
        JavaPairRDD<Integer, ItemsetAndTidsCollection> rKm1ToEclatInput = computeEclatInput(currStep, ranks1AndK);
        return computeWithSequentialEclat(rKm1ToEclatInput);
    }

    BigFimResult createResult(JavaRDD<List<long[]>> optionalEclatFis) {
        return new BigFimResult(cxt.itemToRank, cxt.rankToItem, aprioriFis, optionalEclatFis);
    }

    private JavaPairRDD<Integer, ItemsetAndTidsCollection> computeEclatInput(
            AprioriStepRes currStep, JavaRDD<Tuple2<int[], long[]>> ranks1AndK) {
        //prepare the input RDD:
        cxt.pp("Preparing to generate Eclat input");
        JavaRDD<long[]> kRanksBsRdd = ranks1AndK.map(r1AndK -> r1AndK._2);
        unpersistPrevIfNeeded();
        kRanksBsRdd = kRanksBsRdd.persist(StorageLevel.MEMORY_AND_DISK_SER());

        //compute TIDs
        cxt.pp("Computing TIDs");
        TidsGenHelper tidsGenHelper = currStep.constructTidGenHelper(cxt.cnts.totalTrs);
        PairRanks rkToRkm1AndR1 = currStep.currSizeAllRanks.constructRkToRkm1AndR1ForMaxK();
        JavaRDD<long[][]> rankToTidBsRdd = cxt.apr.computeCurrRankToTidBitSet_Part(kRanksBsRdd, tidsGenHelper);
        rankToTidBsRdd = rankToTidBsRdd.persist(StorageLevel.MEMORY_AND_DISK_SER());
        allRanksRdds.get(allRanksRdds.size() - 2).unpersist(); //the last one computed for Eclat should not be persisted
        kRanksBsRdd.unpersist();

        //preparing the input
        cxt.pp("Preparing Eclat input");
        JavaPairRDD<Integer, List<long[]>> rkm1ToTidSets =
                cxt.apr.groupTidSetsByRankKm1(rankToTidBsRdd, rkToRkm1AndR1, props.maxEclatNumParts);
        return rkm1ToTidSets.mapValues(tidSets ->
                TidMergeSet.mergeTidSetsWithSameRankDropMetadata(tidSets, tidsGenHelper, currStep.currSizeAllRanks));
    }

    private JavaRDD<List<long[]>> computeWithSequentialEclat(
            JavaPairRDD<Integer, ItemsetAndTidsCollection> rKm1ToEclatInput) {
        cxt.pp("Starting Eclat computations");
        EclatProperties eclatProps = new EclatProperties(cxt.cnts.minSuppCnt, cxt.totalFreqItems);
        eclatProps.setUseDiffSets(props.isUseDiffSets);
        eclatProps.setSqueezingEnabled(props.isSqueezingEnabled);
        eclatProps.setCountingOnly(props.isCountingOnly);
        eclatProps.setRankToItem(cxt.rankToItem);

        EclatAlg eclat = new EclatAlg(eclatProps);
        JavaRDD<List<long[]>> resRdd = eclat.computeFreqItemsetsRdd(rKm1ToEclatInput);
        cxt.pp("Num parts for Eclat: " + resRdd.getNumPartitions());

        return resRdd;
    }

    private void unpersistPrevIfNeeded() {
        if (allRanksRdds.size() >= 2) {
            allRanksRdds.get(allRanksRdds.size() - 2).unpersist();
        }
    }
}
