package my.apriori;

import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class AprCandidateFisGeneratorTest {
    private static final Random RAND = new Random(10L);
    private AprCandidateFisGenerator<String> gen;

    @Before
    public void setUp() throws Exception {
        gen = new AprCandidateFisGenerator<>();
    }

    @Test
    public void getNextSizeCandItemsets() throws Exception {
        List<List<String>> oldFis = Arrays.asList(
                Arrays.asList("a", "b"),
                Arrays.asList("a", "c"),
                Arrays.asList("a", "d"),
                Arrays.asList("b", "c"),
                Arrays.asList("b", "d"),
                Arrays.asList("b", "e")
        );
        Collections.shuffle(oldFis, RAND);

        Collection<List<String>> newItemsets = gen.getNextSizeCandItemsets(oldFis);

        Collection<List<String>> expItemsets = Arrays.asList(
                Arrays.asList("a", "b", "c"),
                Arrays.asList("a", "b", "d")
        );
        assertThat(newItemsets, is(expItemsets));
    }

    @Test
    public void getNextSizeCandItemsetsFromTransaction() throws Exception {
        //prepare
        Set<String> f1 = new HashSet<>(Arrays.asList("a", "b", "c", "d", "e", "f", "k"));
        Set<List<String>> oldFisK = new HashSet<>(Arrays.asList(
                Arrays.asList("a", "b"),
                Arrays.asList("a", "c"),
                Arrays.asList("a", "d"),
                Arrays.asList("b", "c"),
                Arrays.asList("b", "d"),
                Arrays.asList("b", "e"),
                Arrays.asList("c", "e"),
                Arrays.asList("c", "k"),
                Arrays.asList("d", "e")
        ));
        List<List<String>> trK = Arrays.asList(
                Arrays.asList("a", "b"),
                Arrays.asList("a", "c"),
                Arrays.asList("a", "e"),
                Arrays.asList("a", "f"),
                Arrays.asList("b", "c"),
                Arrays.asList("b", "e"),
                Arrays.asList("b", "f"),
                Arrays.asList("c", "e"),
                Arrays.asList("c", "f"),
                Arrays.asList("e", "f")
        );
        Collections.shuffle(trK, RAND);

        //run
        Collection<List<String>> trKp1 = gen.getNextSizeCandItemsetsFromTransaction(trK, 2, f1, oldFisK);

        //verify
        Collection<List<String>> expTrKp1 = Arrays.asList(
                Arrays.asList("b", "c", "e"),//both bc, be, and ce are frequent and are present in trK
                Arrays.asList("a", "b", "c") //both ab, ac, and bc are frequent and are present in trK
        );
        assertThat(trKp1, is(expTrKp1));
    }

    @Test
    public void genTransactionC2s() throws Exception {
        ArrayList<String> transaction = new ArrayList<>(Arrays.asList("b", "d", "e"));

        Collection<List<String>> c2 = gen.genTransactionC2s(transaction);

        Collection<List<String>> expC2 = Arrays.asList(
                Arrays.asList("b", "d"),
                Arrays.asList("b", "e"),
                Arrays.asList("d", "e")
        );
        assertThat(c2, is(expC2));
    }
}