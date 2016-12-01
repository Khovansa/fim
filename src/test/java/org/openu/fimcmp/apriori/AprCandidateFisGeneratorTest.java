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
}