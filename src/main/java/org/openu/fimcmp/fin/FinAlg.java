package org.openu.fimcmp.fin;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.openu.fimcmp.FreqItemset;
import org.openu.fimcmp.algbase.AlgBase;
import org.openu.fimcmp.algbase.F1Context;
import org.openu.fimcmp.result.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Implement the main steps of the FIN+ algorithm.
 */
public class FinAlg extends AlgBase<FinAlgProperties> {

    public static void main(String[] args) throws Exception {
        FinAlgProperties props = new FinAlgProperties(0.8);
        props.inputNumParts = 1;
        props.isPersistInput = true;
        props.requiredItemsetLenForSeqProcessing = 2;
        FinAlg alg = new FinAlg(props);

        StopWatch sw = new StopWatch();
        sw.start();
        JavaSparkContext sc = createSparkContext(false, "local", sw);

        String inputFile = "C:\\Users\\Alexander\\Desktop\\Data Mining\\DataSets\\" + "my.small.txt";
        JavaRDD<String[]> trs = alg.readInput(sc, inputFile, sw);

        F1Context f1Context = alg.computeF1Context(trs, sw);
        f1Context.printRankToItem();
        JavaRDD<int[]> rankTrsRdd = f1Context.computeRddRanks1(trs);

        //run the algorithm
//        FiResultHolder resultHolder = new BitsetFiResultHolder(f1Context.totalFreqItems, 20_000);
//        alg.collectResultsSequentiallyPureJava(resultHolder, rankTrsRdd, f1Context, props.requiredItemsetLenForSeqProcessing);
        FiResultHolderFactory resultHolderFactory = new BitsetFiResultHolderFactory();
        FiResultHolder resultHolder = alg.collectResultsSequentiallyWithSpark(
                resultHolderFactory, rankTrsRdd, f1Context, props.requiredItemsetLenForSeqProcessing, sc);

        //output the results
        List<FreqItemset> allFrequentItemsets = resultHolder.getAllFrequentItemsets(f1Context.rankToItem);
        System.out.println("Total results: " + allFrequentItemsets.size());
        allFrequentItemsets = allFrequentItemsets.stream().
                sorted(FreqItemset::compareForNiceOutput2).collect(Collectors.toList());
        for (FreqItemset freqItemset : allFrequentItemsets) {
            System.out.println(freqItemset);
        }
        System.out.println("Total results: " + allFrequentItemsets.size());
    }

    public FinAlg(FinAlgProperties props) {
        super(props);
    }

    public static Class[] getClassesToRegister() {
        return new Class[] {
                FinAlgProperties.class, ProcessedNodeset.class, DiffNodeset.class, PpcNode.class,
        };
    }

    FiResultHolder collectResultsSequentiallyWithSpark(
            FiResultHolderFactory resultHolderFactory, JavaRDD<int[]> rankTrsRdd, F1Context f1Context,
            int requiredItemsetLenForSeqProcessing, JavaSparkContext sc) {

        FiResultHolder rootsResultHolder = resultHolderFactory.newResultHolder(f1Context.totalFreqItems, 20_000);
        List<ProcessedNodeset> rootNodesets =
                createAscFreqSortedRoots(rootsResultHolder, rankTrsRdd, f1Context, requiredItemsetLenForSeqProcessing);
        Collections.reverse(rootNodesets); //nodes sorted in descending frequency, to start the most frequent ones first
        JavaRDD<ProcessedNodeset> rootsRdd = sc.parallelize(rootNodesets);

        //process each subtree
        FiResultHolder initResultHolder = resultHolderFactory.newResultHolder(f1Context.totalFreqItems, 20_000);
        final int totalFreqItems = f1Context.totalFreqItems;
        final long minSuppCnt = f1Context.minSuppCnt;
        FiResultHolder subtreeResultHolder = rootsRdd.map(
                pn -> pn.processSubtree(resultHolderFactory, totalFreqItems, 10_000, minSuppCnt))
                .fold(initResultHolder, FiResultHolder::uniteWith);

        return rootsResultHolder.uniteWith(subtreeResultHolder);
    }

    void collectResultsSequentiallyPureJava(
            FiResultHolder resultHolder, JavaRDD<int[]> rankTrsRdd, F1Context f1Context,
            int requiredItemsetLenForSeqProcessing) {

        List<ProcessedNodeset> rootNodesets = createAscFreqSortedRoots(
                resultHolder, rankTrsRdd, f1Context, requiredItemsetLenForSeqProcessing);

        //process each subtree
        for (ProcessedNodeset rootNodeset : rootNodesets) {
            rootNodeset.processSubtree(resultHolder, f1Context.minSuppCnt);
        }
    }

    private List<ProcessedNodeset> createAscFreqSortedRoots(
            FiResultHolder resultHolder, JavaRDD<int[]> rankTrsRdd, F1Context f1Context,
            int requiredItemsetLenForSeqProcessing) {
        //create PpcTree
        List<int[]> trsList = rankTrsRdd.collect();
        PpcTree root = new PpcTree(new PpcNode(0));
        for (int[] sortedTr : trsList) {
            root.insertTransaction(sortedTr);
        }
        root.updatePreAndPostOrderNumbers(1, 1);
//        root.print(f1Context.rankToItem, "", null);

        //create root nodesets
        f1Context.updateByF1(resultHolder);

        ArrayList<ArrayList<PpcNode>> itemToPpcNodes = root.getPreOrderItemToPpcNodes(f1Context.totalFreqItems);
        ArrayList<DiffNodeset> sortedF1Nodesets = DiffNodeset.createF1NodesetsSortedByAscFreq(itemToPpcNodes);
        return prepareAscFreqSortedRoots(resultHolder, sortedF1Nodesets, f1Context.minSuppCnt, requiredItemsetLenForSeqProcessing);
    }

    /**
     * @param ascFreqSortedF1    F1 sorted in increasing frequency
     * @param minSuppCnt         -
     * @param requiredItemsetLen the required itemset length of the returned nodes, e.g. '1' for individual items. <br/>
     *                           Note that each node will contain sons, i.e. '1' means a node for an individual frequent
     *                           item + its sons representing frequent pairs.
     */
    private List<ProcessedNodeset> prepareAscFreqSortedRoots(
            FiResultHolder resultHolder, ArrayList<DiffNodeset> ascFreqSortedF1,
            long minSuppCnt, int requiredItemsetLen) {

        List<ProcessedNodeset> roots = DiffNodeset.createProcessedNodesLevel1(ascFreqSortedF1, minSuppCnt);
        for (int currItemsetLen = 1; currItemsetLen < requiredItemsetLen; ++currItemsetLen) {
            roots = createNextLevel(resultHolder, roots, minSuppCnt);
        }
        return roots;
    }

    private List<ProcessedNodeset> createNextLevel(
            FiResultHolder resultHolder, List<ProcessedNodeset> roots, long minSuppCnt) {
        ArrayList<ProcessedNodeset> res = new ArrayList<>(ProcessedNodeset.countSons(roots));
        for (ProcessedNodeset root : roots) {
            res.addAll(root.processSonsOnly(resultHolder, minSuppCnt));
        }
        return res;
    }
}
