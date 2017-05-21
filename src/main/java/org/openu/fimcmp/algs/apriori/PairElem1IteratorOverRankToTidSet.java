package org.openu.fimcmp.algs.apriori;

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
    private final long[][] rankToTidSet;
    private final PairRanks rankToPair;
    private int currRankK = -1;

    public PairElem1IteratorOverRankToTidSet(long[][] rankToTidSet, PairRanks rankToPair) {
        this.rankToTidSet = rankToTidSet;
        this.rankToPair = rankToPair;
    }

    @Override
    public boolean hasNext() {
        return currRankK + 1 < rankToTidSet.length;
    }

    /**
     * @return tuple (elem1, TidMergeSet), where (elem1, elem2) is a pair mapped by 'currRankK'.
     */
    @Override
    public Tuple2<Integer, long[]> next() {
        ++currRankK;
        int elem1 = rankToPair.getElem1ByRank(currRankK);
        Assert.isTrue(elem1 >= 0);
        long[] tidSet = rankToTidSet[currRankK];
        if (tidSet == null) {
            tidSet = TidMergeSet.newEmptySet(currRankK);
        }
        Tuple2<Integer, long[]> res = new Tuple2<>(elem1, tidSet);
        return res;
    }

//    private void skipNulls() {
//        while (currRankK+1 < rankToTidSet.length && rankToTidSet[currRankK + 1] == null) {
//            ++currRankK;
//        }
//    }
}
