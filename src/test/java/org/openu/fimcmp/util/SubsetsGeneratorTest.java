package org.openu.fimcmp.util;

import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

public class SubsetsGeneratorTest {

    @Test
    public void test() {
        for (int setSize = 0; setSize<=10; ++setSize) {
            List<int[]> res = new ArrayList<>(1000);
            SubsetsGenerator.generateAllSubsets(res, setSize);
            assertThat(""+setSize, res.size(), is((int)SubsetsGenerator.getNumberOfAllSubsets(setSize)));
        }
    }

    @Ignore
    @Test
    public void print() {
        List<int[]> res = new ArrayList<>(1000);
        int setSize = 6;
        for (int subsetSize =0; subsetSize<=setSize; ++subsetSize) {
            res.clear();
            SubsetsGenerator.generateAllSubsetsOfSize(res, setSize, subsetSize);
            printRes(res, setSize, subsetSize);
            System.out.println("");
        }
    }

    private void printRes(List<int[]> res, int setSize, int subsetSize) {
        System.out.println(String.format("[%s, %s] -> %s subsets", subsetSize, setSize, res.size()));
        for (int[] subSet : res) {
            System.out.println(Arrays.toString(subSet));
        }
    }
}