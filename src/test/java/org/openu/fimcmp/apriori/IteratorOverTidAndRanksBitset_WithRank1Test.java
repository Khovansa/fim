package org.openu.fimcmp.apriori;

import org.junit.Before;
import org.junit.Test;
import org.openu.fimcmp.util.BitArrays;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.openu.fimcmp.apriori.IteratorOverTidAndRanksBitset_WithRank1.START_IND;

public class IteratorOverTidAndRanksBitset_WithRank1Test {
    private static final long TID = 17L;
    private static final int RANK1 = 5;

    private Rank1Provider rank1Provider;

    @Before
    public void setUp() throws Exception {
        rank1Provider = mock(Rank1Provider.class);
        when(rank1Provider.getRank1(anyInt())).thenReturn(RANK1);
    }

    @Test
    public void normal_case() {
        long[] words = new long[24];
        words[0] = TID;

        int[] ranks = {0, 1, 7, 63, 64, 65, 128, 192, 200, 213, 214, 1034, 1234};
        List<Tuple3<Integer, Integer, Long>> expRes = new ArrayList<>(ranks.length);
        for (int rank : ranks) {
            BitArrays.set(words, START_IND, rank);
            expRes.add(new Tuple3<>(RANK1, rank, TID));
        }

        IteratorOverTidAndRanksBitset_WithRank1 it = new IteratorOverTidAndRanksBitset_WithRank1(words, rank1Provider);
        List<Tuple3<Integer, Integer, Long>> actRes = new ArrayList<>(ranks.length);
        while (it.hasNext()) {
            actRes.add(it.next());
        }

        assertThat(actRes, is(expRes));
    }

    @Test(timeout = 2000L, expected = Exception.class)
    public void no_hang_on_next() {
        long[] words = new long[2];
        words[0] = TID;
        BitArrays.set(words, START_IND, 5);
        IteratorOverTidAndRanksBitset_WithRank1 it = new IteratorOverTidAndRanksBitset_WithRank1(words, rank1Provider);

        assertThat(it.next(), is(new Tuple3<>(RANK1, 5, TID)));
        it.next();
    }

    @Test
    public void no_ranks() {
        long[] words = new long[1];
        words[0] = TID;

        IteratorOverTidAndRanksBitset_WithRank1 it = new IteratorOverTidAndRanksBitset_WithRank1(words, rank1Provider);
        assertFalse(it.hasNext());
    }

    @Test
    public void empty() {
        long[] words = new long[0];

        IteratorOverTidAndRanksBitset_WithRank1 it = new IteratorOverTidAndRanksBitset_WithRank1(words, rank1Provider);
        assertFalse(it.hasNext());
    }
}