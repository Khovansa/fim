package org.openu.fimcmp.fin;

import org.openu.fimcmp.result.FiResultHolder;
import org.openu.fimcmp.util.Assert;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

class PatternTreeNode implements Serializable {
    private final DiffNodeset diffNodeset;
    private final PatternTreeNode parent;
    private ArrayList<Integer> equivalentItems = null;
    private PatternTreeNode[] lastItemToSon = null;


    PatternTreeNode(DiffNodeset diffNodeset, PatternTreeNode parent) {
        Assert.isTrue(diffNodeset != null);
        this.diffNodeset = diffNodeset;
        this.parent = parent;
    }

    static List<PatternTreeNode> createAndCountLevel2(
            FiResultHolder resultHolder, ArrayList<DiffNodeset> ascSortedF1, int minSuppCnt) {
        final int totalFreqItems = ascSortedF1.size();
        ArrayList<PatternTreeNode> secondLevelNodes = new ArrayList<>(totalFreqItems * (totalFreqItems - 1));
        for (int ix = 0; ix< totalFreqItems; ++ix) {
            DiffNodeset xSet = ascSortedF1.get(ix);
            PatternTreeNode newRoot = new PatternTreeNode(xSet, null);
            for (int iy = ix+1; iy< totalFreqItems; ++iy) {
                DiffNodeset ySet = ascSortedF1.get(iy);
                DiffNodeset xySet = DiffNodeset.constructForItemPair(xSet, ySet);
                if (xySet.getSupportCnt() < minSuppCnt) {
                    continue;
                }
                PatternTreeNode level2Node = newRoot.addSon(xySet, totalFreqItems);
                secondLevelNodes.add(level2Node);
                resultHolder.addFrequentItemset(level2Node.diffNodeset.getSupportCnt(), level2Node.diffNodeset.getItemset());
            }
        }
        secondLevelNodes.trimToSize();
        return secondLevelNodes;
    }

    ArrayList<Integer> createAndCountSons(
            FiResultHolder resultHolder, List<Integer> sortedMoreFrequentItems, Param param) {
        Assert.isTrue(equivalentItems == null);
        Assert.isTrue(lastItemToSon == null);
        Assert.isTrue(parent != null);
        Assert.isTrue(parent.lastItemToSon != null);

        //create all the sons
        ArrayList<Integer> newNodeLastItems = extendNodeByItems(sortedMoreFrequentItems, param);

        //produce the output from the current node
        resultHolder.addClosedItemset(
                diffNodeset.getSupportCnt(), diffNodeset.getItemset(), parent.equivalentItems, equivalentItems);

        return newNodeLastItems;
    }

    void processSubtree(
            FiResultHolder resultHolder, ArrayList<Integer> newNodeLastItems, Param param) {
        for (int ii=0; ii<newNodeLastItems.size(); ++ii) {
            int newItem = newNodeLastItems.get(ii);
            List<Integer> moreFreqItems = newNodeLastItems.subList(ii+1, newNodeLastItems.size());
            PatternTreeNode son = lastItemToSon[newItem];
            ArrayList<Integer> sonLastItems =
                    son.createAndCountSons(resultHolder, moreFreqItems, param);
            son.processSubtree(resultHolder, sonLastItems, param);
            lastItemToSon[newItem] = null; //done with this one, let's free the memory
        }
    }

    /**
     * Extend the tree by extending the current node. <br/>
     * See lines 1-20 of 'Constructing_Pattern_Tree()' procedure of the DiffNodesets algorithm paper.
     */
    private ArrayList<Integer> extendNodeByItems(List<Integer> sortedMoreFrequentItems, Param param) {
        Assert.isTrue(parent != null);
        ArrayList<Integer> newNodeItems = new ArrayList<>(sortedMoreFrequentItems.size());

        for (Integer extItem : sortedMoreFrequentItems) {
            PatternTreeNode yNode = parent.lastItemToSon[extItem];
            if (yNode != null) {
                extendNodeByOneItem(yNode.diffNodeset, newNodeItems, param);
            }
        }

        return newNodeItems;
    }

    /**
     * Extend the tree by considering 'y'. <br/>
     * See lines 5-17 of 'Constructing_Pattern_Tree()' procedure of the DiffNodesets algorithm paper.
     */
    private void extendNodeByOneItem(DiffNodeset y, ArrayList<Integer> newNodeLastItems, Param param) {
        final int i = y.lastItem();
        final DiffNodeset x = diffNodeset;
        final DiffNodeset p = DiffNodeset.constructByDiff(x, y);

        final int pSupportCnt = p.getSupportCnt();
        if (pSupportCnt == x.getSupportCnt()) {
            equivalentItems = addToSortedIfNotExists(equivalentItems, i);
        } else if (pSupportCnt >= param.minSuppCnt) {
            addSon(p, param.totalFreqItems);
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

    private PatternTreeNode addSon(DiffNodeset newNodeset, int totalFreqItems) {
        if (lastItemToSon == null) {
            lastItemToSon = new PatternTreeNode[totalFreqItems];
        }

        int lastItem = newNodeset.lastItem();
        PatternTreeNode newSon = new PatternTreeNode(newNodeset, this);
        lastItemToSon[lastItem] = newSon;
        return newSon;
    }

    static class Param implements Serializable {
        final int minSuppCnt;
        final int totalFreqItems;

        Param(int minSuppCnt, int totalFreqItems) {
            this.minSuppCnt = minSuppCnt;
            this.totalFreqItems = totalFreqItems;
        }
    }

}
