package org.openu.fimcmp.util;

import java.util.Arrays;
import java.util.List;

/**
 * Allows to generate all subsets of indices for a set of indices {0, 1, ... n}. <br/>
 */
public class SubsetsGenerator {

    public static long getNumberOfAllSubsets(int setSize) {
        return (1L<<setSize);
    }

    public static void generateAllSubsets(List<int[]> result, int setSize) {
        for (int subsetSize = 0; subsetSize<= setSize; ++subsetSize) {
            generateAllSubsetsOfSize(result, setSize, subsetSize);
        }
    }

    public static void generateAllSubsetsOfSize(List<int[]> result, int setSize, int subsetSize) {
        if (subsetSize > setSize) {
            String errMsg = String.format("Subset size %s greater than the set size %s", subsetSize, setSize);
            throw new IllegalArgumentException(errMsg);
        }

        int[] currSubset = new int[subsetSize]; // indices holder

        // first index sequence: 0, 1, 2, ...
        for (int i = 0; i < subsetSize; ++i) {
            currSubset[i] = i;
        }
        result.add(copyOf(currSubset));

        for(int incPos = findPositionToIncrement(currSubset, setSize, subsetSize); incPos >= 0;
            incPos = findPositionToIncrement(currSubset, setSize, subsetSize)) {

            ++currSubset[incPos];  // increment this item
            for (++incPos; incPos < subsetSize; ++incPos) {    // fill up remaining items
                currSubset[incPos] = currSubset[incPos - 1] + 1;
            }
            result.add(copyOf(currSubset));
        }
    }

    // find position of item that can be incremented
    private static int findPositionToIncrement(int[] currSubset, int setSize, int subsetSize) {
        int incPos = subsetSize - 1;
        while(incPos >= 0 && currSubset[incPos] == setSize - subsetSize + incPos) {
            --incPos;
        }
        return incPos;
    }

    private static int[] copyOf(int[] s) {
        return Arrays.copyOf(s, s.length);
    }
}
