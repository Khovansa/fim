package org.openu.fimcmp.apriori;

import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;

import static org.junit.Assert.*;

public class TidMergeSetTest {
    @Test
    public void tmp() {
        final long totalTids = 180L;
        final int rank = 17;
        long[] res = TidMergeSet.mergeElem(new long[]{}, new Tuple2<>(rank, 23L), totalTids);
        res = TidMergeSet.mergeElem(res, new Tuple2<>(rank, 0L), totalTids);
        res = TidMergeSet.mergeElem(res, new Tuple2<>(rank, 2L), totalTids);
        res = TidMergeSet.mergeElem(res, new Tuple2<>(rank, 5L), totalTids);
        res = TidMergeSet.mergeElem(res, new Tuple2<>(rank, 179L), totalTids);
        res = TidMergeSet.mergeElem(res, new Tuple2<>(rank, 129L), totalTids);
        res = TidMergeSet.mergeElem(res, new Tuple2<>(rank, 128L), totalTids);

        long[] res2 = TidMergeSet.mergeElem(new long[]{}, new Tuple2<>(rank, 23L), totalTids);
        res2 = TidMergeSet.mergeElem(res2, new Tuple2<>(rank, 64L), totalTids);
        res2 = TidMergeSet.mergeElem(res2, new Tuple2<>(rank, 65L), totalTids);
        res2 = TidMergeSet.mergeElem(res2, new Tuple2<>(rank, 66L), totalTids);
        res2 = TidMergeSet.mergeElem(res2, new Tuple2<>(rank, 127L), totalTids);
        System.out.println("s1:");
        System.out.println(Arrays.toString(res));
        System.out.println(Long.toBinaryString(res[(int)res[2]]));
        System.out.println(Long.toBinaryString(res[(int)res[3]]));
        System.out.println("s2:");
        System.out.println(Arrays.toString(res2));
        System.out.println(Long.toBinaryString(res2[(int)res2[2]]));
        System.out.println(Long.toBinaryString(res2[(int)res2[3]]));

        res = TidMergeSet.mergeSets(res, res2);
        res = TidMergeSet.mergeSets(new long[]{}, res);
        System.out.println("After merge:");
        System.out.println(Arrays.toString(res));
        System.out.println(Long.toBinaryString(res[(int)res[2]]));
        System.out.println(Long.toBinaryString(res[(int)res[3]]));
    }

}