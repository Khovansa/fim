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
 * Helper methods for the FinAlg class.
 */
class FinAlgHelper implements Serializable {

    /**
     * Generate 'group-dependent/conditional transactions'. <br/>
     * Copy-paste from FPGrowth.genCondTransactions(). <br/>
     * @return list of (group-id, conditional-transaction)
     */
    static Iterator<Tuple2<Integer, int[]>> genCondTransactions(
            int[] ascSortedTr, Partitioner partitioner) {
        final int numParts = partitioner.numPartitions();
        Map<Integer, int[]> resPartToSlice = new HashMap<>(numParts * 2);
        for (int i = ascSortedTr.length - 1; i >= 0; --i) {
            int itemRank = ascSortedTr[i];
            int part = partitioner.getPartition(itemRank);
            if (!resPartToSlice.containsKey(part)) {
                int[] slice = Arrays.copyOf(ascSortedTr, i + 1);
                resPartToSlice.put(part, slice);
            }
        }

        List<Tuple2<Integer, int[]>> res = new ArrayList<>(resPartToSlice.size());
        for (Map.Entry<Integer, int[]> entry : resPartToSlice.entrySet()) {
            res.add(new Tuple2<>(entry.getKey(), entry.getValue()));
        }
        return res.iterator();
    }

    /**
     * The core of PFIN+ algorithm.
     */
    static FiResultHolder findAllFisByParallelFin(
            JavaPairRDD<Integer, int[]> partToCondTrsRdd, Partitioner partitioner,
            FiResultHolderFactory resultHolderFactory, F1Context f1Context, FinAlgProperties props) {

        FiResultHolder rootsResultHolder = resultHolderFactory.newResultHolder();

        PpcTree emptyTree = PpcTree.emptyTree();
        //Generate PpcTree per partition.
        //Note that transaction's items are stored in ascending order, i.e. in decreasing frequency
        JavaPairRDD<Integer, PpcTree> partAndTreeRdd = partToCondTrsRdd
                .aggregateByKey(emptyTree, partitioner, PpcTree::insertTransaction, PpcTree::merge)
                .mapValues(PpcTree::withUpdatedPreAndPostOrderNumbers);

        //Generate all FIs from the PpcTree objects
        long minSuppCnt = f1Context.minSuppCnt;
        int totalFreqItems = f1Context.totalFreqItems;
        FiResultHolder initResultHolder = resultHolderFactory.newResultHolder();
        FiResultHolder subtreeResultHolder = partAndTreeRdd
                .map(partAndTree -> FinAlgHelper.genAllFisForPartition(
                        resultHolderFactory, partAndTree, minSuppCnt, totalFreqItems, props, partitioner))
                .fold(initResultHolder, FiResultHolder::uniteWith);

        return rootsResultHolder.uniteWith(subtreeResultHolder);
    }

    /**
     * Only prepare the roots using a <b>single</b> PpcTree that should be kept in memory. <br/>
     * The FI mining could then proceed either sequentially or in parallel.
     */
    static List<ProcessedNodeset> createAscFreqSortedRoots(
            FiResultHolder resultHolder, JavaRDD<int[]> rankTrsRdd, F1Context f1Context,
            int requiredItemsetLenForSeqProcessing) {

        //create the single PpcTree
        PpcTree root = createRoot(rankTrsRdd);
//        root.print(f1Context.rankToItem, "", null);

        //create root nodesets
        ArrayList<ArrayList<PpcNode>> itemToPpcNodes = root.getPreOrderItemToPpcNodes(f1Context.totalFreqItems);
        ArrayList<DiffNodeset> sortedF1Nodesets = DiffNodeset.createF1NodesetsSortedByAscFreq(itemToPpcNodes);
        return prepareAscFreqSortedRoots(
                resultHolder, sortedF1Nodesets, f1Context.minSuppCnt, requiredItemsetLenForSeqProcessing, null);
    }

    private static PpcTree createRoot(JavaRDD<int[]> rankTrsRdd) {
        PpcTree root = PpcTree.emptyTree();
        root = rankTrsRdd.aggregate(root, PpcTree::insertTransaction, PpcTree::merge);

        return root.withUpdatedPreAndPostOrderNumbers();
    }

    private static FiResultHolder genAllFisForPartition(
            FiResultHolderFactory resultHolderFactory,
            Tuple2<Integer, PpcTree> partAndTreeRoot,
            long minSuppCnt,
            int totalFreqItems,
            FinAlgProperties props,
            Partitioner partitioner) {

        /*
         (1) In PFPGrowth they only look for patterns ending in i | part(i)=gid, because that's their idea:
             if FI ends at i | part(i)=gid, then it can be counted solely in the group-dependent shard of this gid
         (2) We should do the same in Nodesets: only look for FIs ending at i | part(i)=gid
             In any Nodeset(i1,...ik), i1 is the least frequent item so the sorted FI would end in i1 in the PpcTree.
             So for each group-dependent shard we should only take Nodeset(i1...ik) | part(i1) = gid.
         (3) Let i1 denote an item for which part(i1) = gid.
             For the group-dependent shard with this gid,
             we should *only include Nodeset(i1, i) and exclude the rest, i is any item*
         */
        final Integer part = partAndTreeRoot._1;
        System.out.println("Starting FIs for partition " + part);
        Predicate<Integer> leastFreqItemFilter = (itemRank -> partitioner.getPartition(itemRank) == part);

        final PpcTree root = partAndTreeRoot._2;
        FiResultHolder res = genAllFisForPartition(resultHolderFactory, root, leastFreqItemFilter, minSuppCnt, totalFreqItems, props);
        System.out.println(String.format("Completed FIs for partition %s: %s", part, res.size()));
        return res;
    }

    private static FiResultHolder genAllFisForPartition(
            FiResultHolderFactory resultHolderFactory,
            PpcTree root, Predicate<Integer> leastFreqItemFilter,
            long minSuppCnt, int totalFreqItems, FinAlgProperties props) {

        FiResultHolder resultHolder = resultHolderFactory.newResultHolder();

        ArrayList<ArrayList<PpcNode>> itemToPpcNodes = root.getPreOrderItemToPpcNodes(totalFreqItems);
        ArrayList<DiffNodeset> ascFreqSortedF1 = DiffNodeset.createF1NodesetsSortedByAscFreq(itemToPpcNodes);
        List<ProcessedNodeset> rootNodesets = prepareAscFreqSortedRoots(
                resultHolder, ascFreqSortedF1, minSuppCnt, props.requiredItemsetLenForSeqProcessing, leastFreqItemFilter);
        //nodes sorted in descending frequency, to start the most frequent ones first:
        Collections.reverse(rootNodesets);

        System.out.println(String.format("Starting processing subtrees: %s roots", rootNodesets.size()));
        for (ProcessedNodeset rootNodeset : rootNodesets) {
            System.out.println(String.format("Processing subtree of %s", Arrays.toString(rootNodeset.getItemset())));
            int sizeBefore = resultHolder.size();

            rootNodeset.processSubtree(resultHolder, minSuppCnt);

            System.out.println(String.format("Done processing subtree of %s: +%s -> %s",
                    Arrays.toString(rootNodeset.getItemset()), resultHolder.size() - sizeBefore, resultHolder.size()));
        }

        return resultHolder;
    }

    /**
     * @param ascFreqSortedF1     F1 sorted in increasing frequency
     * @param minSuppCnt          -
     * @param requiredItemsetLen  the required itemset length of the returned nodes, e.g. '1' for individual items. <br/>
     *                            Note that each node will contain sons, i.e. '1' means a node for an individual frequent
     * @param leastFreqItemFilter optional
     */
    private static List<ProcessedNodeset> prepareAscFreqSortedRoots(
            FiResultHolder resultHolder, ArrayList<DiffNodeset> ascFreqSortedF1,
            long minSuppCnt, int requiredItemsetLen, Predicate<Integer> leastFreqItemFilter) {

        List<ProcessedNodeset> roots =
                DiffNodeset.createProcessedNodesLevel1(resultHolder, ascFreqSortedF1, minSuppCnt, leastFreqItemFilter);
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
