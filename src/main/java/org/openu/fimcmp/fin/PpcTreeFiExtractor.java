package org.openu.fimcmp.fin;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.openu.fimcmp.algbase.F1Context;
import org.openu.fimcmp.result.FiResultHolder;
import org.openu.fimcmp.result.FiResultHolderFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;
import java.util.function.Predicate;

/**
 * Extract all frequent itemsets from the given PpcTree
 */
class PpcTreeFiExtractor implements Serializable {
    private final PpcTree root;
    private final Predicate<Integer> validate2ndNode;

    PpcTreeFiExtractor(PpcTree root, Predicate<Integer> validate2ndNode) {
        this.root = root;
        this.validate2ndNode = validate2ndNode;
    }

    static Iterator<Tuple2<Integer, int[]>> genCondTransactions(
            int[] ascSortedTr, Partitioner partitioner) {
        int numParts = partitioner.numPartitions();
        Map<Integer, int[]> resPartToSlice = new HashMap<>(numParts * 2);
        for (int i=ascSortedTr.length-1; i>=0; --i) {
            int itemRank = ascSortedTr[i];
            int part = partitioner.getPartition(itemRank);
            if (!resPartToSlice.containsKey(part)) {
                int[] slice = Arrays.copyOf(ascSortedTr, i+1);
                resPartToSlice.put(part, slice);
            }
        }

        List<Tuple2<Integer, int[]>> res = new ArrayList<>(resPartToSlice.size());
        for (Map.Entry<Integer, int[]> entry : resPartToSlice.entrySet()) {
            res.add(new Tuple2<>(entry.getKey(), entry.getValue()));
        }
        return res.iterator();
    }

    static PpcTree createRoot(JavaRDD<int[]> rankTrsRdd) {
        PpcTree root = PpcTree.emptyTree();
        //TODO - fix using merge
//        root = rankTrsRdd.aggregate(root, PpcTree::insertTransaction, PpcTree::merge);
        List<int[]> trsList = rankTrsRdd.collect();
        for (int[] sortedTr : trsList) {
            root.insertTransaction(sortedTr);
        }

        return root.withUpdatedPreAndPostOrderNumbers();
    }

    static FiResultHolder findAllFis(
            JavaPairRDD<Integer, int[]> partToRankTrsRdd, Partitioner partitioner,
            FiResultHolderFactory resultHolderFactory, F1Context f1Context, FinAlgProperties props) {
        FiResultHolder rootsResultHolder = resultHolderFactory.newResultHolder(f1Context.totalFreqItems, 10_000);
        f1Context.updateByF1(rootsResultHolder);

        PpcTree emptyTree = PpcTree.emptyTree();
        JavaPairRDD<Integer, PpcTree> partAndTreeRdd =
                partToRankTrsRdd.aggregateByKey(emptyTree, partitioner, PpcTree::insertTransaction, PpcTree::merge).
                mapValues(PpcTree::withUpdatedPreAndPostOrderNumbers);

        long minSuppCnt = f1Context.minSuppCnt;
        int totalFreqItems = f1Context.totalFreqItems;
        FiResultHolder initResultHolder = resultHolderFactory.newResultHolder(f1Context.totalFreqItems, 20_000);
        FiResultHolder subtreeResultHolder = partAndTreeRdd
                .map(partAndTree -> PpcTreeFiExtractor.genAllFisForPartition(
                        resultHolderFactory, minSuppCnt, totalFreqItems, props, partitioner, partAndTree))
                .fold(initResultHolder, FiResultHolder::uniteWith);

        return rootsResultHolder.uniteWith(subtreeResultHolder);
    }

    private static FiResultHolder genAllFisForPartition(
            FiResultHolderFactory resultHolderFactory,
            long minSuppCnt,
            int totalFreqItems,
            FinAlgProperties props,
            Partitioner partitioner,
            Tuple2<Integer, PpcTree> partAndTreeRoot) {
        Integer part = partAndTreeRoot._1;
        PpcTree root = partAndTreeRoot._2;

        Predicate<Integer> validate2ndNode = (itemRank -> partitioner.getPartition(itemRank) == part);
        PpcTreeFiExtractor fiExtractor = new PpcTreeFiExtractor(root, validate2ndNode);

        FiResultHolder resultHolder = resultHolderFactory.newResultHolder(totalFreqItems, 10_000);
        fiExtractor.genAllFisForPartition(resultHolder, minSuppCnt, totalFreqItems, props);
        return resultHolder;
    }

    private void genAllFisForPartition(
            FiResultHolder resultHolder,
            long minSuppCnt,
            int totalFreqItems,
            FinAlgProperties props) {

        //TODO - only take nodesets whose i2 matches 'validate2ndNode' predicate
        ArrayList<ArrayList<PpcNode>> itemToPpcNodes = root.getPreOrderItemToPpcNodes(totalFreqItems);
        ArrayList<DiffNodeset> ascFreqSortedF1 = DiffNodeset.createF1NodesetsSortedByAscFreq(itemToPpcNodes);
        List<ProcessedNodeset> rootNodesets = prepareAscFreqSortedRoots(
                resultHolder, ascFreqSortedF1, minSuppCnt, props.requiredItemsetLenForSeqProcessing);
        Collections.reverse(rootNodesets); //nodes sorted in descending frequency, to start the most frequent ones first

        for (ProcessedNodeset rootNodeset : rootNodesets) {
            rootNodeset.processSubtree(resultHolder, minSuppCnt);
        }
    }

    /**
     * @param ascFreqSortedF1    F1 sorted in increasing frequency
     * @param minSuppCnt         -
     * @param requiredItemsetLen the required itemset length of the returned nodes, e.g. '1' for individual items. <br/>
     *                           Note that each node will contain sons, i.e. '1' means a node for an individual frequent
     *                           item + its sons representing frequent pairs.
     */
    static List<ProcessedNodeset> prepareAscFreqSortedRoots(
            FiResultHolder resultHolder, ArrayList<DiffNodeset> ascFreqSortedF1,
            long minSuppCnt, int requiredItemsetLen) {

        List<ProcessedNodeset> roots = DiffNodeset.createProcessedNodesLevel1(ascFreqSortedF1, minSuppCnt);
        for (int currItemsetLen = 1; currItemsetLen < requiredItemsetLen; ++currItemsetLen) {
            roots = createNextLevel(resultHolder, roots, minSuppCnt);
        }
        return roots;
    }

    private static List<ProcessedNodeset> createNextLevel(
            FiResultHolder resultHolder, List<ProcessedNodeset> roots, long minSuppCnt) {
        ArrayList<ProcessedNodeset> res = new ArrayList<>(ProcessedNodeset.countSons(roots));
        for (ProcessedNodeset root : roots) {
            res.addAll(root.processSonsOnly(resultHolder, minSuppCnt));
        }
        return res;
    }
}
