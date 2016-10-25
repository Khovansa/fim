package org.openu.fimcmp.util;

import java.util.Iterator;

/**
 * Unsafe iterator over the supplied array. <br/>
 * It is unsafe since it does not copy the array and does not check for its modification. <br/>
 * {@link #remove()} operation is not supported.
 */
public class IteratorOverArray<V> implements Iterator<V> {
    private final V[] arr;
    private int ii = -1;

    public IteratorOverArray(V[] arr) {
        this.arr = arr;
    }

    @Override
    public boolean hasNext() {
        return ii < arr.length - 1;
    }

    @Override
    public V next() {
        ++ii;
        return arr[ii];
    }
}
