package org.openu.fimcmp.apriori;

import org.junit.Test;

import java.util.Arrays;

public class TidMergeSetTest {
    @Test
    public void tmp() {
        final long totalTids = 180L;
        final int rank = 17;
        long[] res = TidMergeSet.mergeElem(new long[]{}, new long[]{rank, 23L}, totalTids);
        res = TidMergeSet.mergeElem(res, new long[]{rank, 0L}, totalTids);
        res = TidMergeSet.mergeElem(res, new long[]{rank, 2L}, totalTids);
        res = TidMergeSet.mergeElem(res, new long[]{rank, 5L}, totalTids);
        res = TidMergeSet.mergeElem(res, new long[]{rank, 179L}, totalTids);
        res = TidMergeSet.mergeElem(res, new long[]{rank, 129L}, totalTids);
        res = TidMergeSet.mergeElem(res, new long[]{rank, 128L}, totalTids);

        long[] res2 = TidMergeSet.mergeElem(new long[]{}, new long[]{rank, 23L}, totalTids);
        res2 = TidMergeSet.mergeElem(res2, new long[]{rank, 64L}, totalTids);
        res2 = TidMergeSet.mergeElem(res2, new long[]{rank, 65L}, totalTids);
        res2 = TidMergeSet.mergeElem(res2, new long[]{rank, 66L}, totalTids);
        res2 = TidMergeSet.mergeElem(res2, new long[]{rank, 127L}, totalTids);
        res2 = TidMergeSet.mergeElem(res2, new long[]{rank, 126L}, totalTids);
        res2 = TidMergeSet.mergeElem(res2, new long[]{rank, 125L}, totalTids);
        res2 = TidMergeSet.mergeElem(res2, new long[]{rank, 124L}, totalTids);
        res2 = TidMergeSet.mergeElem(res2, new long[]{rank, 0L}, totalTids);
        res2 = TidMergeSet.mergeElem(res2, new long[]{rank, 1L}, totalTids);
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

        System.out.println(TidMergeSet.toNormalListOfTids(res, 100));
    }

}