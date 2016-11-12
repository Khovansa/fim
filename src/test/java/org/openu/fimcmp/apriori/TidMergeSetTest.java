package org.openu.fimcmp.apriori;

import org.junit.Test;

import static org.junit.Assert.*;

public class TidMergeSetTest {
    @Test
    public void tmp() {
        System.out.println(String.format("%s", Long.toBinaryString(-1L)));
        System.out.println(String.format("%s", Long.toBinaryString((2L<<6)-1)));
        System.out.println(String.format("%s", TidMergeSet.getIndex(131L))); //receiving index by input number
        System.out.println(String.format("%s", TidMergeSet.getRemainder(131L))); //remainder
        System.out.println(String.format("%s", Long.toBinaryString(TidMergeSet.getRemainderAsBit(131L)))); //bit set
    }

}