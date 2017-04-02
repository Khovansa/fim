package org.openu.fimcmp.fin;

class PpcNode extends PcNode {
    private int postOrder;

    PpcNode() {
    }

    public int getPostOrder() {
        return postOrder;
    }

    public void setPostOrder(int postOrder) {
        this.postOrder = postOrder;
    }
}
