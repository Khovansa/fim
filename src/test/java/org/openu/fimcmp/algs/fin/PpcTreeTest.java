package org.openu.fimcmp.algs.fin;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

public class PpcTreeTest {
    @Test
    public void test_basic_operations() {
        PpcTree root = new PpcTree(new PpcNode(0));
        root.insertTransaction(new int[]{0, 1, 2, 3});
        root.insertTransaction(new int[]{0, 5});
        root.insertTransaction(new int[]{0, 1, 3, 4});
        root.insertTransaction(new int[]{1, 2, 3, 4, 5});
        root.updatePreAndPostOrderNumbers(1, 1);

        assertThat(root.getBy(), is(new PpcNode(0, 1, 13)));
        assertThat(root.getBy(0, 1), is(new PpcNode(2, 3, 5)));
        assertThat(root.getBy(1), is(new PpcNode(1, 9, 12)));
        assertThat(root.getBy(1, 2), is(new PpcNode(1, 10, 11)));
        assertThat(root.getBy(1, 2, 3), is(new PpcNode(1, 11, 10)));
        assertThat(root.getBy(1, 2, 3, 4), is(new PpcNode(1, 12, 9)));
        assertThat(root.getBy(1, 2, 3, 4, 5), is(new PpcNode(1, 13, 8)));
    }

    @Test
    public void test_merge() {
        PpcTree tree1 = PpcTree.emptyTree();
        tree1.insertTransaction(new int[]{0, 1, 2, 3});
        tree1.insertTransaction(new int[]{0, 5});
        tree1.insertTransaction(new int[]{0, 1, 3, 4});
        tree1.insertTransaction(new int[]{1, 2, 3, 4, 5});

        PpcTree tree2 = PpcTree.emptyTree();
        tree2.insertTransaction(new int[]{0, 1, 2}); //shorter than orig pattern
        tree2.insertTransaction(new int[]{0, 5, 6}); //longer thank orig pattern
        tree2.insertTransaction(new int[]{0, 5});
        tree2.insertTransaction(new int[]{2, 3}); //new pattern

        tree1.merge(tree2);

        //verify
        assertThat(tree1.getBy(), is(new PpcNode(0)));
        assertThat(tree1.getBy(0), is(new PpcNode(6)));
        assertThat(tree1.getBy(0, 1), is(new PpcNode(3)));
        assertThat(tree1.getBy(0, 1, 2), is(new PpcNode(2)));
        assertThat(tree1.getBy(0, 1, 2, 3), is(new PpcNode(1)));
        assertThat(tree1.getBy(0, 5), is(new PpcNode(3)));
        assertThat(tree1.getBy(0, 5, 6), is(new PpcNode(1)));
        assertThat(tree1.getBy(2), is(new PpcNode(1)));
        assertThat(tree1.getBy(2, 3), is(new PpcNode(1)));
        //tree2 should not change
        assertThat(tree2.getBy(0), is(new PpcNode(3)));
        assertThat(tree2.getBy(0, 1), is(new PpcNode(1)));
        assertThat(tree2.getBy(0, 1, 2), is(new PpcNode(1)));
        assertThat(tree2.getBy(0, 5), is(new PpcNode(2)));
        assertThat(tree2.getBy(2), is(new PpcNode(1)));
        assertThat(tree2.getBy(2, 3), is(new PpcNode(1)));
    }

    @SuppressWarnings("unused")
    private static void printItemsetNode(PpcTree root, int... itemset) {
        PpcNode node = root.getBy(itemset);
        System.out.println(String.format("%-20s: %s", Arrays.toString(itemset), node));
    }
}