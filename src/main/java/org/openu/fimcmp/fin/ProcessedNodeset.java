package org.openu.fimcmp.fin;

import org.apache.commons.collections.CollectionUtils;
import org.openu.fimcmp.result.FiResultHolder;
import org.openu.fimcmp.result.FiResultHolderFactory;
import org.openu.fimcmp.util.Assert;

import java.io.Serializable;
import java.util.*;

/**
 * Replacement of the 'pattern tree node' in the original FIN+ algorithm. <br/>
 * It seems to be unnecessary to keep the entire tree in memory. <br/>
 * To process a node we only need its parent and its right siblings. <br/>
 */
class ProcessedNodeset implements Serializable {
    private final DiffNodeset diffNodeset;
    private ArrayList<Integer> equivalentItems = null;
    private LinkedList<DiffNodeset> sons = null;

    ProcessedNodeset(DiffNodeset diffNodeset) {
        Assert.isTrue(diffNodeset != null);
        this.diffNodeset = diffNodeset;
    }

    private void updateResult(FiResultHolder resultHolder, List<Integer> parentEquivItems) {
        resultHolder.addClosedItemset(
                diffNodeset.getSupportCnt(), diffNodeset.getItemset(), parentEquivItems, equivalentItems);
    }

    List<ProcessedNodeset> processSonsOnly(FiResultHolder resultHolder, long minSuppCnt) {
        if (CollectionUtils.isEmpty(sons)) {
            return Collections.emptyList();
        }

        List<ProcessedNodeset> res = new ArrayList<>(sons.size());
        while (!sons.isEmpty()) {
            DiffNodeset son = sons.pop();
            ProcessedNodeset processedSon = son.createProcessedNode(sons, minSuppCnt);
            processedSon.updateResult(resultHolder, equivalentItems);
            res.add(processedSon);
        }

        return res;
    }

    FiResultHolder processSubtree(
            FiResultHolderFactory resultHolderFactory, long minSuppCnt) {
        FiResultHolder resultHolder = resultHolderFactory.newResultHolder();
        processSubtree(resultHolder, minSuppCnt);
        return resultHolder;
    }

    /**
     * The main method for the parallel algorithm's 'map' phase. <br/>
     * Collect results from each son and its subtree. <br/>
     */
    void processSubtree(FiResultHolder resultHolder, long minSuppCnt) {
        if (CollectionUtils.isEmpty(sons)) {
            return;
        }

        while (!sons.isEmpty()) {
            DiffNodeset son = sons.pop();
            ProcessedNodeset processedSon = son.createProcessedNode(sons, minSuppCnt);
            processedSon.updateResult(resultHolder, equivalentItems);
            System.out.println(String.format("Processing subtree of %s", Arrays.toString(processedSon.getItemset())));
            processedSon.processSubtree(resultHolder, minSuppCnt);
        }
    }

    static int countSons(List<ProcessedNodeset> nodes) {
        int res = 0;
        for (ProcessedNodeset node : nodes) {
            if (node.sons != null) {
                res += node.sons.size();
            }
        }
        return res;
    }

    void addNewEquivItem(int item) {
        if (equivalentItems == null) {
            equivalentItems = new ArrayList<>(2);
        }
        equivalentItems.add(item);
    }

    void addSon(DiffNodeset son) {
        if (sons == null) {
            sons = new LinkedList<>();
        }
        sons.add(son);
    }

    int[] getItemset() {
        return diffNodeset.getItemset();
    }
}
