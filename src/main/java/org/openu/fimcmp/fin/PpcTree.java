package org.openu.fimcmp.fin;

import scala.Tuple2;

import java.util.Map;
import java.util.TreeMap;

/**
 * Tree of PpcNode instances.
 */
class PpcTree {
    private final PpcNode currNode;
    private Map<Integer, PpcTree> itemToChildNode;

    PpcTree(PpcNode currNode) {
        this.currNode = currNode;
    }

    void insertTransaction(int[] sortedTr, int currInd) {
        if (currInd >= sortedTr.length) {
            return; //recursion end
        }

        PpcTree child = insertItem(sortedTr[currInd]);
        child.insertTransaction(sortedTr, currInd + 1);
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

    PpcNode getBy(int... sortedTr) {
        return getBy(sortedTr, 0);
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
        if (itemToChildNode == null) {
            itemToChildNode = new TreeMap<>();
        }

        PpcTree child = itemToChildNode.get(item);
        if (child != null) {
            child.currNode.incCount();
        } else {
            child = new PpcTree(new PpcNode(1));
            itemToChildNode.put(item, child);
        }

        return child;
    }
}
