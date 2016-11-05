package org.openu.fimcmp.apriori;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class AprCandidateFisGeneratorTest {
    //    private static final Random RAND = new Random(10L);
    private AprCandidateFisGenerator gen;

    @Before
    public void setUp() throws Exception {
        gen = new AprCandidateFisGenerator();
    }

//    @Test
//    public void getNextSizeCandItemsets() throws Exception {
//        List<List<String>> oldFis = Arrays.asList(
//                Arrays.asList("a", "b"),
//                Arrays.asList("a", "c"),
//                Arrays.asList("a", "d"),
//                Arrays.asList("b", "c"),
//                Arrays.asList("b", "d"),
//                Arrays.asList("b", "e")
//        );
//        Collections.shuffle(oldFis, RAND);
//
//        Collection<List<String>> newItemsets = gen.getNextSizeCandItemsets(oldFis);
//
//        Collection<List<String>> expItemsets = Arrays.asList(
//                Arrays.asList("a", "b", "c"),
//                Arrays.asList("a", "b", "d")
//        );
//        assertThat(newItemsets, is(expItemsets));
//    }

//    @Test
//    public void getNextSizeCandItemsetsFromTransaction() throws Exception {
//        //prepare
//        Set<String> f1 = new HashSet<>(Arrays.asList("a", "b", "c", "d", "e", "f", "k"));
//        Set<List<String>> oldFisK = new HashSet<>(Arrays.asList(
//                Arrays.asList("a", "b"),
//                Arrays.asList("a", "c"),
//                Arrays.asList("a", "d"),
//                Arrays.asList("b", "c"),
//                Arrays.asList("b", "d"),
//                Arrays.asList("b", "e"),
//                Arrays.asList("c", "e"),
//                Arrays.asList("c", "k"),
//                Arrays.asList("d", "e")
//        ));
//        List<List<String>> trK = Arrays.asList(
//                Arrays.asList("a", "b"),
//                Arrays.asList("a", "c"),
//                Arrays.asList("a", "e"),
//                Arrays.asList("a", "f"),
//                Arrays.asList("b", "c"),
//                Arrays.asList("b", "e"),
//                Arrays.asList("b", "f"),
//                Arrays.asList("c", "e"),
//                Arrays.asList("c", "f"),
//                Arrays.asList("e", "f")
//        );
//        Collections.shuffle(trK, RAND);
//
//        //run
//        Collection<List<String>> trKp1 = gen.getNextSizeCandItemsetsFromTransaction(trK, 2, f1, oldFisK);
//
//        //verify
//        Collection<List<String>> expTrKp1 = Arrays.asList(
//                Arrays.asList("b", "c", "e"),//both bc, be, and ce are frequent and are present in trK
//                Arrays.asList("a", "b", "c") //both ab, ac, and bc are frequent and are present in trK
//        );
//        assertThat(trKp1, is(expTrKp1));
//    }

//    @Test
//    public void genTransactionC2s() throws Exception {
//        ArrayList<String> transaction = new ArrayList<>(Arrays.asList("b", "d", "e"));
//
//        Collection<List<String>> c2 = gen.genTransactionC2s(transaction);
//
//        Collection<List<String>> expC2 = Arrays.asList(
//                Arrays.asList("b", "d"),
//                Arrays.asList("b", "e"),
//                Arrays.asList("d", "e")
//        );
//        assertThat(c2, is(expC2));
//    }

    @Test
    public void genTransactionC2sNew() throws Exception {
        int[] transaction = {0, 3, 6, 9};
        int[][] c2 = gen.genTransactionC2s(transaction);

        int[][] expC2 = {
                {0, 3, 1, 6, 1, 9, 1},
                {3, 6, 1, 9, 1},
                {6, 9, 1}
        };

        assertTrue(Arrays.deepToString(c2), Arrays.deepEquals(expC2, c2));
    }

    @Test
    public void mergeC2Columns_diffValues() throws Exception {
        int[] col1 = {0, 3, 1, 6, 1, 9, 2};
        int[] col2 = {0, 1, 1, 2, 1, 3, 2, 5, 1, 6, 1};

        int[] res = gen.mergeC2Columns(col1, col2);

        int[] expRes = {0, 1, 1, 2, 1, 3, 3, 5, 1, 6, 2, 9, 2};
        assertThat(res, is(expRes));
    }

    @Test
    public void mergeC2Columns_sameValues() throws Exception {
        int[] col1 = {0, 3, 1, 6, 1, 9, 2};
        int[] col2 = {0, 3, 2, 6, 1, 9, 3};

        int[] res = gen.mergeC2Columns(col1, col2);

        int[] expRes = {0, 3, 3, 6, 2, 9, 5};
        assertThat(res, is(expRes));
    }

    @Test
    public void mergeC2Columns_firstEmpty() throws Exception {
        int[] col1 = {};
        int[] col2 = {0, 3, 2, 6, 1, 9, 3};

        int[] res = gen.mergeC2Columns(col1, col2);

        assertThat(res, is(col2));
        assertThat(res, not(sameInstance(col2)));
    }

    @Test
    public void mergeC2Columns_bothEmpty() throws Exception {
        int[] col1 = {};
        int[] col2 = {};

        int[] res = gen.mergeC2Columns(col1, col2);

        assertThat(res, is(col2));
    }

    @Test
    public void mergeC2Columns_col2IsSubsetOfCol1_noTail() throws Exception {
        int[] col1 = {0, 19, 2, 20, 2, 21, 2};
        int[] col2 = {0, 19, 1, 21, 1};

        int[] res = gen.mergeC2Columns(col1, col2);

        int[] expRes = {0, 19, 3, 20, 2, 21, 3};
        assertThat(res, is(expRes));
    }

    @Test
    public void mergeC2Columns_col2IsSubsetOfCol1_withTail() throws Exception {
        int[] col1 = {0, 19, 2, 20, 2, 21, 2, 22, 1};
        int[] col2 = {0, 19, 1, 21, 1};

        int[] res = gen.mergeC2Columns(col1, col2);

        int[] expRes = {0, 19, 3, 20, 2, 21, 3, 22, 1};
        assertThat(res, is(expRes));
    }

    @Test
    public void getC2sFilteredByMinSupport_normal() throws Exception {
        int[] col = {0, 3, 1, 6, 3, 9, 2};

        assertThat(gen.getC2sFilteredByMinSupport(col, 1), is(col));
        assertThat(gen.getC2sFilteredByMinSupport(col, 2), is(new int[]{0, 6, 3, 9, 2}));
        assertThat(gen.getC2sFilteredByMinSupport(col, 3), is(new int[]{0, 6, 3}));
        assertThat(gen.getC2sFilteredByMinSupport(col, 4), is(new int[]{}));
    }

    @Test
    public void getC2sFilteredByMinSupport_empty() throws Exception {
        int[] col = {};
        assertThat(gen.getC2sFilteredByMinSupport(col, 1), is(col));
    }

    @Test
    public void f2ColToPairs_normal() throws Exception {
        int[] col = {0, 3, 1, 6, 3, 9, 2};

        int[][] pairs = gen.fkColToPairs(col).toArray(new int[][]{});

        int[][] expPairs = {{0, 3, 1}, {0, 6, 3}, {0, 9, 2}};
        assertTrue(Arrays.deepToString(pairs), Arrays.deepEquals(expPairs, pairs));
    }

    @Test
    public void f2ColToPairs_singleElem() throws Exception {
        int[] col = {0};

        int[][] pairs = gen.fkColToPairs(col).toArray(new int[][]{});

        int[][] expPairs = {};
        assertTrue(Arrays.deepToString(pairs), Arrays.deepEquals(expPairs, pairs));
    }

    @Test
    public void f2ColToPairs_empty() throws Exception {
        int[] col = {};

        int[][] pairs = gen.fkColToPairs(col).toArray(new int[][]{});

        int[][] expPairs = {};
        assertTrue(Arrays.deepToString(pairs), Arrays.deepEquals(expPairs, pairs));
    }
}