package org.openu.fimcmp.fin;

import java.util.ArrayList;
import java.util.List;

class PatternTreeNode {
    private final DiffNodeset diffNodeset;
    private final PatternTreeNode parent;
    private ArrayList<Integer> equivalentItems = new ArrayList<>(3);
    private List<PatternTreeNode> sons = new ArrayList<>(5);


    PatternTreeNode(DiffNodeset diffNodeset, PatternTreeNode parent) {
        this.diffNodeset = diffNodeset;
        this.parent = parent;
    }

    /**
     * Extend the tree by considering 'y'. <br/>
     * See lines 5-17 of 'Constructing_Pattern_Tree()' procedure of the DiffNodesets algorithm paper.
     */
    void addSonIfNeeded(DiffNodeset y, int minSuppCnt, ArrayList<Integer> resNextCadSet) {
        final int i = y.lastItem();
        final DiffNodeset x = diffNodeset;
        final DiffNodeset p = DiffNodeset.constructByDiff(x, y);

        final int pSupportCnt = p.getSupportCnt();
        if (pSupportCnt == x.getSupportCnt()) {
            addToSortedIfNotExists(equivalentItems, i);
        } else if (pSupportCnt >= minSuppCnt) {
            sons.add(new PatternTreeNode(p, this));
            addToSortedIfNotExists(resNextCadSet, i);
        }

    }

    private static void addToSortedIfNotExists(ArrayList<Integer> resNextCadSet, int i) {
        if (resNextCadSet.isEmpty() || i != resNextCadSet.get(resNextCadSet.size()-1)) {
            resNextCadSet.add(i);
        }
    }
}
