package org.openu.fimcmp.util;

import java.io.Serializable;
import java.util.BitSet;

/**
 * Fast and efficient fixed-size set of pairs of non-negative integers. <br/>
 * The range of the elements of the pair is assumed to be [0, MAX1] and [0, MAX2] respectively. <br/>
 * The implementation is based on fixed-size Java BitSet. <br/>
 */
public class BoundedIntPairSet implements Serializable {
    private final int max1;
    private final int max2;
    private final int mult1;
    private final BitSet bitSet;

    public BoundedIntPairSet(int maxElem1, int maxElem2) {
        Assert.isTrue(maxElem1>0 && maxElem2>0);
        this.max1 = maxElem1;
        this.max2 = maxElem2;
        this.mult1 = (maxElem2 + 1);
        int capacity = (maxElem1 + 1) * mult1;
        Assert.isTrue(capacity > 0); //no overflow
        this.bitSet = new BitSet(capacity);
    }

    public void add(int k1, int k2) {
        bitSet.set(toActKey(k1, k2));
    }

    public boolean contains(int k1, int k2) {
        return bitSet.get(toActKey(k1, k2));
    }

    public void remove(int k1, int k2) {
        bitSet.clear(toActKey(k1, k2));
    }

    private Integer toActKey(int k1, int k2) {
        Assert.isTrue(k1 >= 0 && k1 <= max1 && k2 >= 0 && k2 <= max2);
        return k1 * mult1 + k2;
    }
}
