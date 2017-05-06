package org.openu.fimcmp.fin;

import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

/**
 * Tree of PpcNode instances.
 */
class PpcTree implements Serializable {
    private final PpcNode currNode;
    private Map<Integer, PpcTree> itemToChildNode;

    PpcTree(PpcNode currNode) {
        this.currNode = currNode;
    }

    static PpcTree emptyTree() {
        return new PpcTree(new PpcNode(0));
    }

    /**
     * @param sortedTr transaction ranks, sorted in descending frequency (e.g. in ascending natural order)
     */
    PpcTree insertTransaction(int[] sortedTr) {
        insertTransaction(sortedTr, 0);
        return this;
    }

    PpcTree merge(PpcTree other) {
        other.copySubtreeTo(this);
        return this;
    }

    private void insertTransaction(int[] sortedTr, int currInd) {
        if (currInd >= sortedTr.length) {
            return; //recursion end
        }

        PpcTree child = insertItem(sortedTr[currInd]);
        child.insertTransaction(sortedTr, currInd + 1);
    }

    PpcTree withUpdatedPreAndPostOrderNumbers() {
        updatePreAndPostOrderNumbers(1, 1);
        return this;
    }

    Tuple2<Integer, Integer> updatePreAndPostOrderNumbers(int nextPreOrder, int nextPostOrder) {
        currNode.setPreOrder(nextPreOrder++);
        if (itemToChildNode != null) {
            for (PpcTree child : itemToChildNode.values()) {
                Tuple2<Integer, Integer> nextNums = child.updatePreAndPostOrderNumbers(nextPreOrder, nextPostOrder);
                nextPreOrder = nextNums._1;
                nextPostOrder = nextNums._2;
            }
        }
        currNode.setPostOrder(nextPostOrder++);
        return new Tuple2<>(nextPreOrder, nextPostOrder);
    }

    ArrayList<ArrayList<PpcNode>> getPreOrderItemToPpcNodes(int totalFreqItems) {
        ArrayList<ArrayList<PpcNode>> resItemToPpcNodes = new ArrayList<>(totalFreqItems);
        for (int ii=0; ii<totalFreqItems; ++ii) {
            resItemToPpcNodes.add(new ArrayList<>(2));
        }

        preOrderCollectItemToPpcNodes(resItemToPpcNodes);

        return resItemToPpcNodes;
    }

    private void preOrderCollectItemToPpcNodes(ArrayList<ArrayList<PpcNode>> resItemToPpcNodes) {
        if (itemToChildNode != null) {
            for (Map.Entry<Integer, PpcTree> entry : itemToChildNode.entrySet()) {
                int item = entry.getKey();
                PpcTree child = entry.getValue();

                resItemToPpcNodes.get(item).add(child.currNode);
                child.preOrderCollectItemToPpcNodes(resItemToPpcNodes);
            }
        }
    }

    PpcNode getBy(int... sortedTr) {
        return getBy(sortedTr, 0);
    }

    @SuppressWarnings({"WeakerAccess", "unused"})
    void print(String[] rankToItem, String pref, Integer itemRank) {
        String item = (itemRank != null) ? rankToItem[itemRank] : "";
        System.out.println(String.format("%s%s<%s>:%s", pref, itemRank, item, currNode));
        if (itemToChildNode != null) {
            for (Map.Entry<Integer, PpcTree> entry : itemToChildNode.entrySet()) {
                PpcTree son = entry.getValue();
                int sonItemRank = entry.getKey();
                son.print(rankToItem, pref+"\t", sonItemRank);
            }
        }
    }

    private void copySubtreeTo(PpcTree destCurrNode) {
        if (itemToChildNode == null) {
            return;
        }

        for (Map.Entry<Integer, PpcTree> entry : itemToChildNode.entrySet()) {
            int item = entry.getKey();
            PpcTree child = entry.getValue();
            PpcTree destChild = destCurrNode.insertItem(item, child.currNode.getCount());
            child.copySubtreeTo(destChild);
        }
    }

    private PpcNode getBy(int[] sortedTr, int currInd) {
        if (currInd < sortedTr.length) {
            PpcTree child = (itemToChildNode != null) ? itemToChildNode.get(sortedTr[currInd]) : null;
            return (child != null) ? child.getBy(sortedTr, currInd+1) : null;
        } else if (currInd == sortedTr.length){
            return currNode;
        } else {
            return null;
        }
    }

    private PpcTree insertItem(int item) {
        return insertItem(item, 1);
    }

    private PpcTree insertItem(int item, int cnt) {
        if (itemToChildNode == null) {
            itemToChildNode = new TreeMap<>();
        }

        PpcTree child = itemToChildNode.get(item);
        if (child != null) {
            child.currNode.incCount(cnt);
        } else {
            child = new PpcTree(new PpcNode(cnt));
            itemToChildNode.put(item, child);
        }

        return child;
    }
}
