package org.openu.fimcmp.fin;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.openu.fimcmp.algbase.AlgBase;
import org.openu.fimcmp.algbase.F1Context;
import org.openu.fimcmp.result.FiResultHolder;

import java.util.ArrayList;
import java.util.List;

/**
 * Implement the main steps of the FIN+ algorithm.
 */
public class FinAlg extends AlgBase<FinAlgProperties> {

    public static void main(String[] args) throws Exception {
        //TODO
        FinAlgProperties props = new FinAlgProperties(0.5);
        props.inputNumParts = 1;
        props.isPersistInput = true;
        FinAlg alg = new FinAlg(props);

        StopWatch sw = new StopWatch();
        sw.start();
        JavaSparkContext sc = createSparkContext(false, "local", sw);

        String inputFile = "C:\\Users\\Alexander\\Desktop\\Data Mining\\DataSets\\" + "1.txt";
        JavaRDD<String[]> trs = alg.readInput(sc, inputFile, sw);

        F1Context f1Context = alg.computeF1Context(trs, sw);
        JavaRDD<int[]> rankTrsRdd = f1Context.computeRddRanks1(trs);
        List<int[]> trsList = rankTrsRdd.collect();

        //create PpcTree
        PpcTree root = new PpcTree(new PpcNode(0));
        for (int[] sortedTr : trsList) {
            root.insertTransaction(sortedTr);
        }
        root.updatePreAndPostOrderNumbers(1, 1);

        //
    }

    public FinAlg(FinAlgProperties props) {
        super(props);
    }

    /**
     * @param ascFreqSortedF1        F1 sorted in increasing frequency
     * @param minSuppCnt         -
     * @param requiredItemsetLen the required itemset length of the returned nodes, e.g. '1' for individual items. <br/>
     *                           Note that each node will contain sons, i.e. '1' means a node for an individual frequent
     *                           item + its sons representing frequent pairs.
     */
    List<ProcessedNodeset> prepareRoots(
            FiResultHolder resultHolder, ArrayList<DiffNodeset> ascFreqSortedF1,
            int minSuppCnt, int requiredItemsetLen) {

        List<ProcessedNodeset> roots = DiffNodeset.createProcessedNodesLevel1(ascFreqSortedF1, minSuppCnt);
        for (int currItemsetLen = 1; currItemsetLen < requiredItemsetLen; ++currItemsetLen) {
            roots = createNextLevel(resultHolder, roots, minSuppCnt);
        }
        return roots;
    }

    private List<ProcessedNodeset> createNextLevel(
            FiResultHolder resultHolder, List<ProcessedNodeset> roots, int minSuppCnt) {
        ArrayList<ProcessedNodeset> res = new ArrayList<>(ProcessedNodeset.countSons(roots));
        for (ProcessedNodeset root : roots) {
            res.addAll(root.processSonsOnly(resultHolder, minSuppCnt));
        }
        return res;
    }
}
