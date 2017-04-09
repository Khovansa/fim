package org.openu.fimcmp.fin;

import org.openu.fimcmp.result.FiResultHolder;
import org.openu.fimcmp.util.Assert;

import java.util.ArrayList;
import java.util.List;

class PatternTreeNode {
    private final DiffNodeset diffNodeset;
    private final PatternTreeNode parent;
    private ArrayList<Integer> equivalentItems = null;
    private PatternTreeNode[] lastItemToSon = null;


    PatternTreeNode(DiffNodeset diffNodeset, PatternTreeNode parent) {
        Assert.isTrue(diffNodeset != null);
        this.diffNodeset = diffNodeset;
        this.parent = parent;
    }

    void exploreAndProcessSubtree(
            FiResultHolder resultHolder, List<Integer> sortedMoreFrequentItems,
            int minSuppCnt, int totalFreqItems) {
        Assert.isTrue(equivalentItems == null);
        Assert.isTrue(lastItemToSon == null);
        Assert.isTrue(parent != null);
        Assert.isTrue(parent.lastItemToSon != null);

        //create all the sons
        ArrayList<Integer> newNodeLastItems = extendNodeByItems(sortedMoreFrequentItems, minSuppCnt, totalFreqItems);

        //produce the output from the current node
        resultHolder.addClosedItemset(
                diffNodeset.getSupportCnt(), diffNodeset.getItemset(), parent.equivalentItems, equivalentItems);

        //process the sub-tree, eliminating the processed sub-nodes
        for (int ii=0; ii<newNodeLastItems.size(); ++ii) {
            int newItem = newNodeLastItems.get(ii);
            List<Integer> moreFreqItems = newNodeLastItems.subList(ii+1, newNodeLastItems.size());
            lastItemToSon[newItem].exploreAndProcessSubtree(resultHolder, moreFreqItems, minSuppCnt, totalFreqItems);
            lastItemToSon[newItem] = null; //done with this one, let's free the memory
        }
    }

    /**
     * Extend the tree by extending the current node. <br/>
     * See lines 1-20 of 'Constructing_Pattern_Tree()' procedure of the DiffNodesets algorithm paper.
     */
    ArrayList<Integer> extendNodeByItems(
            List<Integer> sortedMoreFrequentItems, int minSuppCnt, int totalFreqItems) {
        Assert.isTrue(parent != null);
        ArrayList<Integer> newNodeItems = new ArrayList<>(sortedMoreFrequentItems.size());

        for (Integer extItem : sortedMoreFrequentItems) {
            PatternTreeNode yNode = parent.lastItemToSon[extItem];
            if (yNode != null) {
                extendNodeByOneItem(yNode.diffNodeset, minSuppCnt, totalFreqItems, newNodeItems);
            }
        }

        return newNodeItems;
    }

    /**
     * Extend the tree by considering 'y'. <br/>
     * See lines 5-17 of 'Constructing_Pattern_Tree()' procedure of the DiffNodesets algorithm paper.
     */
    private void extendNodeByOneItem(
            DiffNodeset y, int minSuppCnt, int totalFreqItems, ArrayList<Integer> newNodeLastItems) {
        final int i = y.lastItem();
        final DiffNodeset x = diffNodeset;
        final DiffNodeset p = DiffNodeset.constructByDiff(x, y);

        final int pSupportCnt = p.getSupportCnt();
        if (pSupportCnt == x.getSupportCnt()) {
            equivalentItems = addToSortedIfNotExists(equivalentItems, i);
        } else if (pSupportCnt >= minSuppCnt) {
            addSon(p, totalFreqItems);
            addToSortedIfNotExists(newNodeLastItems, i);
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
