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

    public FinAlg(FinAlgProperties props) {
        super(props);
    }

    public static Class[] getClassesToRegister() {
        return new Class[] {
                FinAlgProperties.class, ProcessedNodeset.class, DiffNodeset.class, PpcNode.class,
        };
    }

    public static void main(String[] args) throws Exception {
        FinAlgProperties props = new FinAlgProperties(0.8);
        props.inputNumParts = 1;
        props.isPersistInput = true;
        props.requiredItemsetLenForSeqProcessing = 2;
        props.runType = FinAlgProperties.RunType.SEQ_SPARK;

        String inputFile = "C:\\Users\\Alexander\\Desktop\\Data Mining\\DataSets\\" + "my.small.txt";
        runAlg(props, inputFile);
    }

    private static void runAlg(FinAlgProperties props, String inputFile) throws Exception {
        FinContext ctx = prepare(props, inputFile);

        FiResultHolder resultHolder;
        switch (props.runType) {
            case SEQ_PURE_JAVA:
                resultHolder = ctx.alg.collectResultsSequentiallyPureJava(
                        ctx.resultHolderFactory, ctx.rankTrsRdd, ctx.f1Context,
                        props.requiredItemsetLenForSeqProcessing);
                break;
            case SEQ_SPARK:
                resultHolder = ctx.alg.collectResultsSequentiallyWithSpark(
                        ctx.resultHolderFactory, ctx.rankTrsRdd, ctx.f1Context,
                        props.requiredItemsetLenForSeqProcessing, ctx.sc);
                break;
            case PAR_SPARK:
                resultHolder = ctx.alg.collectResultsInParallel(
                        ctx.resultHolderFactory, ctx.rankTrsRdd, ctx.f1Context,
                        props.requiredItemsetLenForSeqProcessing, ctx.sc);
                break;
            default: throw new IllegalArgumentException("Unsupporter run type " + props.runType);
        }

        outpuResults(resultHolder, ctx.f1Context);
    }

    private static FinContext prepare(FinAlgProperties props, String inputFile) throws Exception {
        FinContext res = new FinContext();
        res.alg = new FinAlg(props);

        res.sw = new StopWatch();
        res.sw.start();

        res.sc = createSparkContext(false, "local", res.sw);

        JavaRDD<String[]> trs = res.alg.readInput(res.sc, inputFile, res.sw);

        res.f1Context = res.alg.computeF1Context(trs, res.sw);
        res.f1Context.printRankToItem();
        res.rankTrsRdd = res.f1Context.computeRddRanks1(trs);

        res.resultHolderFactory = new BitsetFiResultHolderFactory();
        return res;
    }

    private FiResultHolder collectResultsInParallel(
            FiResultHolderFactory resultHolderFactory, JavaRDD<int[]> rankTrsRdd,
            F1Context f1Context, int requiredItemsetLenForSeqProcessing, JavaSparkContext sc) {
        //TODO
        FiResultHolder rootsResultHolder = resultHolderFactory.newResultHolder(f1Context.totalFreqItems, 20_000);
        return null;
    }

    private FiResultHolder collectResultsSequentiallyWithSpark(
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

    private FiResultHolder collectResultsSequentiallyPureJava(
            FiResultHolderFactory resultHolderFactory, JavaRDD<int[]> rankTrsRdd, F1Context f1Context,
            int requiredItemsetLenForSeqProcessing) {

        FiResultHolder resultHolder = resultHolderFactory.newResultHolder(f1Context.totalFreqItems, 20_000);
        List<ProcessedNodeset> rootNodesets = createAscFreqSortedRoots(
                resultHolder, rankTrsRdd, f1Context, requiredItemsetLenForSeqProcessing);

        //process each subtree
        for (ProcessedNodeset rootNodeset : rootNodesets) {
            rootNodeset.processSubtree(resultHolder, f1Context.minSuppCnt);
        }

        return resultHolder;
    }

    private static void outpuResults(FiResultHolder resultHolder, F1Context f1Context) {
        List<FreqItemset> allFrequentItemsets = resultHolder.getAllFrequentItemsets(f1Context.rankToItem);
        System.out.println("Total results: " + allFrequentItemsets.size());
        allFrequentItemsets = allFrequentItemsets.stream().
                sorted(FreqItemset::compareForNiceOutput2).collect(Collectors.toList());
        for (FreqItemset freqItemset : allFrequentItemsets) {
            System.out.println(freqItemset);
        }
        System.out.println("Total results: " + allFrequentItemsets.size());
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

    private static class FinContext {
        StopWatch sw;
        JavaSparkContext sc;
        F1Context f1Context;
        FinAlg alg;
        JavaRDD<int[]> rankTrsRdd;
        FiResultHolderFactory resultHolderFactory;
    }
}
