package org.openu.fimcmp.fin;

import org.openu.fimcmp.util.Assert;

import java.util.ArrayList;

class PatternTreeNode {
    private final DiffNodeset diffNodeset;
    private final PatternTreeNode parent;
    private ArrayList<Integer> equivalentItems = null;
    private PatternTreeNode[] lastItemToSon = null;


    PatternTreeNode(DiffNodeset diffNodeset, PatternTreeNode parent) {
        this.diffNodeset = diffNodeset;
        this.parent = parent;
    }

    /**
     * Extend the tree by extending the current node. <br/>
     * See lines 1-20 of 'Constructing_Pattern_Tree()' procedure of the DiffNodesets algorithm paper.
     */
    void extendNodeByItems(ArrayList<Integer> sortedMoreFrequentItems, int minSuppCnt, int totalFreqItems) {
        Assert.isTrue(equivalentItems == null);
        Assert.isTrue(lastItemToSon == null);
        Assert.isTrue(parent != null);
        ArrayList<Integer> resNextCadSet = new ArrayList<>(sortedMoreFrequentItems.size());

        for (Integer extItem : sortedMoreFrequentItems) {
            PatternTreeNode yNode = parent.lastItemToSon[extItem];
            if (yNode != null) {
                extendNodeByOneItem(yNode.diffNodeset, minSuppCnt, totalFreqItems, resNextCadSet);
            }
        }
    }

    /**
     * Extend the tree by considering 'y'. <br/>
     * See lines 5-17 of 'Constructing_Pattern_Tree()' procedure of the DiffNodesets algorithm paper.
     */
    private void extendNodeByOneItem(
            DiffNodeset y, int minSuppCnt, int totalFreqItems, ArrayList<Integer> resNextCadSet) {
        final int i = y.lastItem();
        final DiffNodeset x = diffNodeset;
        final DiffNodeset p = DiffNodeset.constructByDiff(x, y);

        final int pSupportCnt = p.getSupportCnt();
        if (pSupportCnt == x.getSupportCnt()) {
            equivalentItems = addToSortedIfNotExists(equivalentItems, i);
        } else if (pSupportCnt >= minSuppCnt) {
            addSon(p, totalFreqItems);
            addToSortedIfNotExists(resNextCadSet, i);
        }
    }

    private static ArrayList<Integer> addToSortedIfNotExists(ArrayList<Integer> resList, int elem) {
        if (resList == null) {
            resList = new ArrayList<>(2);
        }

        if (resList.isEmpty() || elem != resList.get(resList.size()-1)) {
            resList.add(elem);
        }

        return resList;
    }

    private void addSon(DiffNodeset newNodeset, int totalFreqItems) {
        if (lastItemToSon == null) {
            lastItemToSon = new PatternTreeNode[totalFreqItems];
        }

        int lastItem = newNodeset.lastItem();
        PatternTreeNode newSon = new PatternTreeNode(newNodeset, this);
        lastItemToSon[lastItem] = newSon;
    }
}
