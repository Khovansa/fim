package org.openu.fimcmp.result;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

public class BitsetFiResultHolderTest {
    @Test
    public void uniteWith_merge() {
        BitsetFiResultHolder h1 = new BitsetFiResultHolder(100, 100);
        h1.addFrequentItemset(1, new int[]{1, 2});
        h1.addFrequentItemset(1, new int[]{2, 3, 4});

        BitsetFiResultHolder h2 = new BitsetFiResultHolder(100, 100);
        h2.addFrequentItemset(2, new int[]{2, 3});
        h2.addFrequentItemset(10, new int[]{2, 3, 4});

        BitsetFiResultHolder h3 = new BitsetFiResultHolder(100, 100);
        h3.addFrequentItemset(7, new int[]{2, 3});

        h1 = h1.uniteWith(h2);
        h1 = h1.uniteWith(h3);

        assertThat(h1.size(), is(3L));
        assertThat(h1.getSupportForTest(new int[]{1, 2}), is(1));
        assertThat(h1.getSupportForTest(new int[]{2, 3}), is(9));
        assertThat(h1.getSupportForTest(new int[]{2, 3, 4}), is(11));
        assertThat(h1.getSupportForTest(new int[]{1, 2, 3, 4}), is(0));
        //make sure the other bitsets have not changed:
        assertThat(h2.getSupportForTest(new int[]{2, 3}), is(2));
        assertThat(h3.getSupportForTest(new int[]{2, 3}), is(7));
    }

    @Test
    public void uniteWith_this_is_empty() {
        BitsetFiResultHolder h1 = new BitsetFiResultHolder(100, 100);

        BitsetFiResultHolder h2 = new BitsetFiResultHolder(100, 100);
        h2.addFrequentItemset(2, new int[]{2, 3});
        h2.addFrequentItemset(10, new int[]{2, 3, 4});

        BitsetFiResultHolder h3 = new BitsetFiResultHolder(100, 100);
        h3.addFrequentItemset(7, new int[]{2, 3});

        h1 = h1.uniteWith(h2);
        h1 = h1.uniteWith(h3);

        assertThat(h1.size(), is(2L));
        assertThat(h1.getSupportForTest(new int[]{2, 3}), is(9));
        assertThat(h1.getSupportForTest(new int[]{2, 3, 4}), is(10));
        //make sure the other bitsets have not changed:
        assertThat(h2.getSupportForTest(new int[]{2, 3}), is(2));
        assertThat(h3.getSupportForTest(new int[]{2, 3}), is(7));
    }
}