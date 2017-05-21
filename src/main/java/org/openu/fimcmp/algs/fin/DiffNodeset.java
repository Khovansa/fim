package org.openu.fimcmp.algs.fin;

import org.openu.fimcmp.result.FiResultHolder;
import org.openu.fimcmp.util.Assert;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

class DiffNodeset implements Serializable {
    //Item ranks are sorted in descending order, i.e. from the least to the most frequent:
    private final int[] itemset;
    //Nodes for the 'itemset' sorted by the pre-order in ascending order:
    private final ArrayList<PpcNode> sortedNodes;
    private final int supportCnt;

    private DiffNodeset(int[] itemset, ArrayList<PpcNode> sortedNodes, int supportCnt) {
        this.itemset = itemset;
        this.sortedNodes = sortedNodes;
        this.supportCnt = supportCnt;
    }

    int[] getItemset() {
        return itemset;
    }

    int getSupportCnt() {
        return supportCnt;
    }

    /**
     * Assuming the item ranks start with 0
     */
    static ArrayList<DiffNodeset> createF1NodesetsSortedByAscFreq(ArrayList<ArrayList<PpcNode>> itemToPpcNodes) {
        final int totalFreqItems = itemToPpcNodes.size();
        ArrayList<DiffNodeset> resList = new ArrayList<>(totalFreqItems);
        for (int item = totalFreqItems - 1; item >= 0; --item) {
            ArrayList<PpcNode> ppcNodes = itemToPpcNodes.get(item);
            DiffNodeset nodeset = constructForItem(item, ppcNodes);
            resList.add(nodeset);
        }
        return resList;
    }

    static List<ProcessedNodeset> createProcessedNodesLevel1(
            FiResultHolder resultHolder, ArrayList<DiffNodeset> ascFreqSortedF1, long minSuppCnt, Predicate<Integer> leastFreqItemFilter) {
        final int totalFreqItems = ascFreqSortedF1.size();
        List<ProcessedNodeset> level1Nodes = new ArrayList<>(totalFreqItems);
        for (int ii = 0; ii < totalFreqItems; ++ii) {
            DiffNodeset xSet = ascFreqSortedF1.get(ii);
            // For the group-dependent shard with group id = gid,
            // we should *only include Nodeset(i_gid, i_any) and exclude the rest
            // (i_gid is an item so that part(i_gid)=gid, i_any - any item):
            if (leastFreqItemFilter != null && !leastFreqItemFilter.test(xSet.leastFrequentItem())) {
                continue;
            }
            List<DiffNodeset> rightSiblings = ascFreqSortedF1.subList(ii + 1, ascFreqSortedF1.size());
            ProcessedNodeset level1Node = xSet.createProcessedNode(true, rightSiblings, minSuppCnt, null);
            level1Node.updateResult(resultHolder);
            level1Nodes.add(level1Node);
        }

        return level1Nodes;
    }

    ProcessedNodeset createProcessedNode(List<DiffNodeset> rightSiblings, long minSuppCnt, ArrayList<Integer> optParentEquivItems) {
        return createProcessedNode(false, rightSiblings, minSuppCnt, optParentEquivItems);
    }

    @Override
    public String toString() {
        return String.format("%s<%s>: %s", Arrays.toString(itemset), supportCnt, sortedNodes);
    }

    /**
     * Extend the tree by extending the current node. <br/>
     * See lines 1-20 of 'Constructing_Pattern_Tree()' procedure of the DiffNodesets algorithm paper.
     */
    private ProcessedNodeset createProcessedNode(
            boolean isLevel1, List<DiffNodeset> rightSiblings, long minSuppCnt, ArrayList<Integer> optParentEquivItems) {
        ProcessedNodeset res = new ProcessedNodeset(this);
        res.addNewEquivItems(optParentEquivItems);

        for (DiffNodeset y : rightSiblings) {
            final int i = y.lastItem();
            final DiffNodeset p = constructNew(isLevel1, y);

            final int pSupportCnt = p.getSupportCnt();
            if (pSupportCnt == supportCnt /*&& !isLevel1*/) {
                //There is no need to check for duplicates with 'optParentEquivItems',
                // since an item can be either son or an equivalent item.
                //All the siblings = the parent's sons, 'optParentEquivItems' = the parent's equivalent items.
                //So they are disjoint.
                res.addNewEquivItem(i);
            } else if (pSupportCnt >= minSuppCnt) {
                res.addSon(p);
            }
        }
        return res;
    }

    private DiffNodeset constructNew(boolean isLevel1, DiffNodeset ySet) {
        return (isLevel1) ? constructForItemPair(this, ySet) : constructByDiff(this, ySet);
    }

    /**
     * Creates a DiffNodeset for a single item. <br/>
     */
    private static DiffNodeset constructForItem(int itemRank, ArrayList<PpcNode> itemSortedPpcNodes) {
        int supportCnt = countSum(itemSortedPpcNodes);
        return new DiffNodeset(new int[]{itemRank}, itemSortedPpcNodes, supportCnt);
    }

    /**
     * Create a DiffNodeset from a pair of nodesets. <br/>
     * 'x' should be less frequent than 'y'. <br/>
     * Procedure 'Build_2-itemset_DN()' in the DiffNodesets algorithm paper. <br/>
     */
    private static DiffNodeset constructForItemPair(DiffNodeset xSet, DiffNodeset ySet) {
        Assert.isTrue(xSet.itemset.length == 1);
        Assert.isTrue(ySet.itemset.length == 1);
        int x = xSet.itemset[0];
        int y = ySet.itemset[0];
        Assert.isTrue(x > y);

        ArrayList<PpcNode> resNodes = constructPpcNodeListForItemPair(xSet.sortedNodes, ySet.sortedNodes);

        int supportCnt = xSet.supportCnt - countSum(resNodes);
        return new DiffNodeset(new int[]{x, y}, resNodes, supportCnt);
    }

    private static DiffNodeset constructByDiff(DiffNodeset xSet, DiffNodeset ySet) {
        Assert.isTrue(xSet.itemset.length > 1);
        Assert.isTrue(ySet.itemset.length > 1);
        Assert.isTrue(xSet.itemset.length == ySet.itemset.length);
        Assert.isTrue(xSet.lastItem() > ySet.lastItem());

        int[] resItemset = new int[xSet.itemset.length + 1];
        System.arraycopy(xSet.itemset, 0, resItemset, 0, xSet.itemset.length);
        resItemset[resItemset.length - 1] = ySet.lastItem();

        ArrayList<PpcNode> resNodes = nodesMinus(ySet.sortedNodes, xSet.sortedNodes);

        int supportCnt = xSet.supportCnt - countSum(resNodes);
        return new DiffNodeset(resItemset, resNodes, supportCnt);
    }

    private static ArrayList<PpcNode> constructPpcNodeListForItemPair(
            ArrayList<PpcNode> xSortedNodes, ArrayList<PpcNode> ySortedNodes) {
        final int xLen = xSortedNodes.size();
        final int yLen = ySortedNodes.size();

        ArrayList<PpcNode> resNodes = new ArrayList<>(xLen);
        int xInd = 0, yInd = 0;
        while (xInd < xLen && yInd < yLen) {
            PpcNode nx = xSortedNodes.get(xInd);
            PpcNode ny = ySortedNodes.get(yInd);
            if (nx.getPostOrder() > ny.getPostOrder()) {
                ++yInd; //ny can't be an ancestor of nx and the rest of the 'xSortedNodes' => it's safe to ignore it;
            } else if (nx.getPreOrder() > ny.getPreOrder()) {
                ++xInd; //ny is ancestor of nx => skipping nx
            } else {
                ++xInd;
                resNodes.add(nx); //ny is NOT an ancestor of nx
            }
        }
        resNodes.addAll(xSortedNodes.subList(xInd, xLen));

        resNodes.trimToSize();
        return resNodes;
    }

    /**
     * Simply compute set difference (ySortedNodes - xSortedNodes) using pre-order numbers as element indicators.
     */
    private static ArrayList<PpcNode> nodesMinus(ArrayList<PpcNode> ySortedNodes, ArrayList<PpcNode> xSortedNodes) {
        final int yLen = ySortedNodes.size();
        final int xLen = xSortedNodes.size();

        ArrayList<PpcNode> resNodes = new ArrayList<>(yLen);
        int yInd = 0, xInd = 0;
        while (yInd < yLen && xInd < xLen) {
            PpcNode ny = ySortedNodes.get(yInd);
            PpcNode nx = xSortedNodes.get(xInd);
            int ypo = ny.getPreOrder();
            int xpo = nx.getPreOrder();
            if (xpo < ypo) {
                ++xInd; //nx is behind, so just advance it
            } else if (xpo == ypo) {
                ++yInd; //same node => no insertion
                ++xInd;
            } else {
                resNodes.add(ny); //ypo < xpo => ny is not in the 'xSortedNodes'
                ++yInd;
            }
        }
        resNodes.addAll(ySortedNodes.subList(yInd, yLen));

        resNodes.trimToSize();
        return resNodes;
    }

    private int lastItem() {
        return itemset[itemset.length - 1];
    }

    private int leastFrequentItem() {
        return itemset[0];
    }

    private static int countSum(List<PpcNode> ppcNodes) {
        int res = 0;
        for (PpcNode node : ppcNodes) {
            res += node.getCount();
        }
        return res;
    }
}
