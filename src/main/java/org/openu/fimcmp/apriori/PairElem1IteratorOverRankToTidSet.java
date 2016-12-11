package org.openu.fimcmp.apriori;

import org.openu.fimcmp.util.Assert;
import scala.Tuple2;

import java.util.Iterator;

/**
 * Iterator over the supplied array rankK -> {@link TidMergeSet}. <br/>
 * Each rank is assumed to be mapped to a pair (elem1, elem2). <br/>
 * Returns tuple (elem1, TidMergeSet). <br/>
 * {@link #remove()} operation is not supported.
 */
public class PairElem1IteratorOverRankToTidSet implements Iterator<Tuple2<Integer, long[]>> {
    private final long[][] arr;
    private final PairRanks rankToPair;
    private int currRankK = -1;

    public PairElem1IteratorOverRankToTidSet(long[][] arr, PairRanks rankToPair) {
        this.arr = arr;
        this.rankToPair = rankToPair;
    }

    @Override
    public boolean hasNext() {
        return currRankK < arr.length - 1;
    }

    @Override
    public Tuple2<Integer, long[]> next() {
        ++currRankK;
        int elem1 = rankToPair.getElem1ByRank(currRankK);
        Assert.isTrue(elem1 >= 0);
        return new Tuple2<>(elem1, arr[currRankK]);
    }
}
