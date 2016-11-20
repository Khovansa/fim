package org.openu.fimcmp.apriori;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

public class PairRanksTest {
    @Test
    public void test() throws Exception {
        final int totalElems1 = 6;
        final int totalElems2 = 10;
        List<int[]> sortedPairs = Arrays.asList(
            new int[]{1, 1},
            new int[]{1, 3},
            new int[]{5, 0},
            new int[]{5, 2},
            new int[]{5, 4}
        );
        PairRanks pairRanks = PairRanks.construct(sortedPairs, totalElems1, totalElems2);

        assertTrue(pairRanks.existsPair(1, 1));
        assertTrue(pairRanks.existsPair(5, 0));
        assertFalse(pairRanks.existsPair(1, 0));
        assertFalse(pairRanks.existsPair(5, 3));

        assertThat(pairRanks.totalElems1(), is(totalElems1));
        assertThat(pairRanks.totalElems2(), is(totalElems2));

        assertThat(pairRanks.computeMaxElem1(), is(5));
        assertThat(pairRanks.computeMaxElem2(), is(4));
    }
}
