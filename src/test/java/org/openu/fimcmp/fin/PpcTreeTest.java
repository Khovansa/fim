package org.openu.fimcmp.fin;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

public class PpcTreeTest {
    @Test
    public void test() {
        PpcTree root = new PpcTree(new PpcNode(0));
        root.insertTransaction(new int[]{0, 1, 2, 3}, 0);
        root.insertTransaction(new int[]{0, 5}, 0);
        root.insertTransaction(new int[]{0, 1, 3, 4}, 0);
        root.insertTransaction(new int[]{1, 2, 3, 4, 5}, 0);
        root.updatePreAndPostOrderNumbers(1, 1);

        assertThat(root.getBy(), is(new PpcNode(0, 1, 13)));
        assertThat(root.getBy(0, 1), is(new PpcNode(2, 3, 5)));
        assertThat(root.getBy(1), is(new PpcNode(1, 9, 12)));
        assertThat(root.getBy(1, 2), is(new PpcNode(1, 10, 11)));
        assertThat(root.getBy(1, 2, 3), is(new PpcNode(1, 11, 10)));
        assertThat(root.getBy(1, 2, 3, 4), is(new PpcNode(1, 12, 9)));
        assertThat(root.getBy(1, 2, 3, 4, 5), is(new PpcNode(1, 13, 8)));
    }

    private static void printItemsetNode(PpcTree root, int... itemset) {
        PpcNode node = root.getBy(itemset);
        System.out.println(String.format("%-20s: %s", Arrays.toString(itemset), node));
    }
}