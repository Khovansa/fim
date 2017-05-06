package org.openu.fimcmp.fin;

import java.io.Serializable;
import java.util.Objects;

class PpcNode implements Serializable {
    private int count;
    private int preOrder;
    private int postOrder;

    PpcNode(int count) {
        this.count = count;
    }

    PpcNode(int count, int preOrder, int postOrder) {
        this.count = count;
        this.preOrder = preOrder;
        this.postOrder = postOrder;
    }

    int getCount() {
        return count;
    }

    void incCount(int increment) {
        count += increment;
    }

    int getPreOrder() {
        return preOrder;
    }

    void setPreOrder(int preOrder) {
        this.preOrder = preOrder;
    }

    int getPostOrder() {
        return postOrder;
    }

    void setPostOrder(int postOrder) {
        this.postOrder = postOrder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PpcNode ppcNode = (PpcNode) o;
        return count == ppcNode.count &&
                preOrder == ppcNode.preOrder &&
                postOrder == ppcNode.postOrder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(preOrder);
    }

    @Override
    public String toString() {
        return String.format("%s (%s,%s)", count, preOrder, postOrder);
    }
}
