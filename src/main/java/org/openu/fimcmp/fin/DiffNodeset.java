package org.openu.fimcmp.fin;

import org.openu.fimcmp.util.Assert;

import java.util.ArrayList;
import java.util.List;

class DiffNodeset {
    //Item ranks are sorted in descending order, i.e. from the least to the most frequent:
    private final int[] itemset;
     //Nodes for the 'itemset' and sorted by the pre-order in ascending order:
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