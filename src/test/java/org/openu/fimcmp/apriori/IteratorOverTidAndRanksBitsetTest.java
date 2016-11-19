package org.openu.fimcmp.apriori;

import org.junit.Test;
import org.openu.fimcmp.util.BitArrays;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.openu.fimcmp.apriori.IteratorOverTidAndRanksBitset.START_IND;

public class IteratorOverTidAndRanksBitsetTest {
    private static final long TID = 17L;

    @Test
    public void normal_case() {
        long[] words = new long[24];
        words[0] = TID;

        int[] ranks = {0, 1, 7, 63, 64, 65, 128, 192, 200, 213, 214, 1034, 1234};
        List<Tuple2<Long, Integer>> expRes = new ArrayList<>(ranks.length);
        for (int rank : ranks) {
            BitArrays.set(words, START_IND, rank);
            expRes.add(new Tuple2<>(TID, rank));
        }

        IteratorOverTidAndRanksBitset it = new IteratorOverTidAndRanksBitset(words);
        List<Tuple2<Long, Integer>> actRes = new ArrayList<>(ranks.length);
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
        IteratorOverTidAndRanksBitset it = new IteratorOverTidAndRanksBitset(words);

        assertThat(it.next(), is(new Tuple2<>(TID, 5)));
        it.next();
    }

    @Test
    public void no_ranks() {
        long[] words = new long[1];
        words[0] = TID;

        IteratorOverTidAndRanksBitset it = new IteratorOverTidAndRanksBitset(words);
        assertFalse(it.hasNext());
    }

    @Test
    public void empty() {
        long[] words = new long[0];

        IteratorOverTidAndRanksBitset it = new IteratorOverTidAndRanksBitset(words);
        assertFalse(it.hasNext());
    }
}