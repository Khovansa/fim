package org.openu.fimcmp.algs.apriori;

import org.junit.Before;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class AprCandidateFisGeneratorTest {
    //    private static final Random RAND = new Random(10L);
    private AprCandidateFisGenerator gen;

    @Before
    public void setUp() throws Exception {
        gen = new AprCandidateFisGenerator();
    }
}