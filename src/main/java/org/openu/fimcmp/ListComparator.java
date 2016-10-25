package org.openu.fimcmp;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * Compares lists lexicographically
 */
public class ListComparator<T extends Comparable<T>> implements Comparator<List<T>>, Serializable {
    @Override
    public int compare(List<T> o1, List<T> o2) {
        Iterator<T> i1 = o1.iterator();
        Iterator<T> i2 = o2.iterator();
        int result;

        do {
            if (!i1.hasNext()) {
                return (i2.hasNext()) ? -1 : 0;
            }
            if (!i2.hasNext()) {
                return 1;
            }

            result = i1.next().compareTo(i2.next());
        } while (result == 0);

        return result;
    }
}
