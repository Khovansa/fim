package org.openu.fimcmp.fin;

import org.openu.fimcmp.result.FiResultHolder;
import org.openu.fimcmp.util.Assert;

import java.util.ArrayList;
import java.util.List;

class DiffNodeset {
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

    int lastItem() {
        return itemset[itemset.length - 1];
    }

    int getSupportCnt() {
        return supportCnt;
    }

    /**
     * Creates a DiffNodeset for a single item. <br/>
     */
    static DiffNodeset constructForItem(int itemRank, ArrayList<PpcNode> itemSortedPpcNodes) {
        int supportCnt = countSum(itemSortedPpcNodes);
        return new DiffNodeset(new int[]{itemRank}, itemSortedPpcNodes, supportCnt);
    }

    /**
     *  Create a DiffNodeset from a pair of nodesets. <br/>
     *  'x' should be less frequent than 'y'. <br/>
     *  Procedure 'Build_2-itemset_DN()' in the DiffNodesets algorithm paper. <br/>
     */
    static DiffNodeset constructForItemPair(DiffNodeset xSet, DiffNodeset ySet) {
        Assert.isTrue(xSet.itemset.length == 1);
        Assert.isTrue(ySet.itemset.length == 1);
        int x = xSet.itemset[0];
        int y = ySet.itemset[0];
        Assert.isTrue(x > y);

        ArrayList<PpcNode> resNodes = constructPpcNodeListForItemPair(xSet.sortedNodes, ySet.sortedNodes);

        int supportCnt = xSet.supportCnt - countSum(resNodes);
        return new DiffNodeset(new int[]{x, y}, resNodes, supportCnt);
    }

    static DiffNodeset constructByDiff(DiffNodeset xSet, DiffNodeset ySet) {
        Assert.isTrue(xSet.itemset.length > 1);
        Assert.isTrue(ySet.itemset.length > 1);
        Assert.isTrue(xSet.itemset.length == ySet.itemset.length);
        Assert.isTrue(xSet.lastItem() > ySet.lastItem());

        int[] resItemset = new int[xSet.itemset.length + 1];
        System.arraycopy(xSet.itemset, 0, resItemset, 0, xSet.itemset.length);
        resItemset[resItemset.length - 1] = ySet.lastItem();

        ArrayList<PpcNode> resNodes = nodesMinus(xSet.sortedNodes, ySet.sortedNodes);

        int supportCnt = xSet.supportCnt - countSum(resNodes);
        return new DiffNodeset(resItemset, resNodes, supportCnt);
    }

    static List<ProcessedNodeset> createProcessedNodesLevel1(ArrayList<DiffNodeset> ascSortedF1, int minSuppCnt) {
        final int totalFreqItems = ascSortedF1.size();
        List<ProcessedNodeset> level1Nodes = new ArrayList<>(totalFreqItems);
        for (int ii = 0; ii< totalFreqItems; ++ii) {
            DiffNodeset xSet = ascSortedF1.get(ii);
            List<DiffNodeset> rightSiblings = ascSortedF1.subList(ii+1, ascSortedF1.size());
            ProcessedNodeset level1Node = xSet.createProcessedNode(true, rightSiblings, minSuppCnt);
            level1Nodes.add(level1Node);
        }

        return level1Nodes;
    }

    static void updateResultBy(FiResultHolder resultHolder, List<DiffNodeset> nodes) {
        for (DiffNodeset node : nodes) {
            resultHolder.addFrequentItemset(node.getSupportCnt(), node.getItemset());
        }
    }

    ProcessedNodeset createProcessedNode(List<DiffNodeset> rightSiblings, int minSuppCnt) {
        return createProcessedNode(false, rightSiblings, minSuppCnt);
    }

    /**
     * Extend the tree by extending the current node. <br/>
     * See lines 1-20 of 'Constructing_Pattern_Tree()' procedure of the DiffNodesets algorithm paper.
     */
    private ProcessedNodeset createProcessedNode(boolean isLevel1, List<DiffNodeset> rightSiblings, int minSuppCnt) {
        ProcessedNodeset res = new ProcessedNodeset(this);

        for (DiffNodeset y : rightSiblings) {
            final int i = y.lastItem();
            final DiffNodeset p = constructNew(isLevel1, y);

            final int pSupportCnt = p.getSupportCnt();
            if (pSupportCnt == supportCnt && !isLevel1) {
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

    private static ArrayList<PpcNode> constructPpcNodeListForItemPair(
            ArrayList<PpcNode> xSortedNodes, ArrayList<PpcNode> ySortedNodes) {
        final int xLen = xSortedNodes.size();
        final int yLen = ySortedNodes.size();

        ArrayList<PpcNode> resNodes = new ArrayList<>(xLen);
        int xInd=0, yInd=0;
        while (xInd < xLen && yInd < yLen) {
            PpcNode nx = xSortedNodes.get(xInd);
            PpcNode ny = ySortedNodes.get(yInd);
            if (nx.getPostOrder() > ny.getPostOrder()) {
                ++yInd; //ny can't be an ancestor of nx and the rest of the 'xSortedNodes' => it's safe to ignore it;
            } else if (nx.getPreOrder() > ny.getPreOrder()) {
                ++xInd; //ny is ancestor of nx => skipping nx
            } else {
                resNodes.add(nx); //ny is NOT an ancestor of nx
            }
        }
        resNodes.addAll(xSortedNodes.subList(xInd, xLen));

        resNodes.trimToSize();
        return resNodes;
    }

    private static ArrayList<PpcNode> nodesMinus(ArrayList<PpcNode> xSortedNodes, ArrayList<PpcNode> ySortedNodes) {
        final int xLen = xSortedNodes.size();
        final int yLen = ySortedNodes.size();

        ArrayList<PpcNode> resNodes = new ArrayList<>(xLen);
        int xInd=0, yInd=0;
        while (xInd < xLen && yInd < yLen) {
            PpcNode nx = xSortedNodes.get(xInd);
            PpcNode ny = ySortedNodes.get(yInd);
            int xpo = nx.getPreOrder();
            int ypo = ny.getPreOrder();
            if (xpo > ypo) {
                ++yInd; //ny is behind, so just advance it
            } else if (xpo == ypo) {
                ++xInd; //same node => no insertion
            } else {
                resNodes.add(nx); //xpo < ypo => nx is not in the 'ySortedNodes'
            }
        }
        resNodes.addAll(xSortedNodes.subList(xInd, xLen));

        resNodes.trimToSize();
        return resNodes;
    }

    private static int countSum(List<PpcNode> ppcNodes) {
        int res = 0;
        for (PpcNode node : ppcNodes) {
            res += node.getCount();
        }
        return res;
    }
}
