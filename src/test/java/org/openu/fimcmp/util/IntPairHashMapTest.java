package org.openu.fimcmp.util;

import org.apache.commons.lang3.time.StopWatch;
import org.junit.Test;
import scala.Tuple2;

import java.util.*;

public class IntPairHashMapTest {
    private static final int SAMPLE_SIZE = 10_000_000;
    private static final int MAX_ELEM1 = 200;
    private static final int MAX_ELEM2 = 5000;

    @Test
    public void perf_test_IntPairFixedSet() {
        BoundedIntPairSet map = new BoundedIntPairSet(MAX_ELEM1+1, MAX_ELEM2+1);
        int cnt = 0;
        Random random = new Random(new Date().getTime());
        StopWatch sw = new StopWatch();
        sw.start();
        for (int ii=0; ii<SAMPLE_SIZE; ++ii) {
            int i11 = (int)(random.nextDouble() * MAX_ELEM1);
            int i12 = (int)(random.nextDouble() * MAX_ELEM2);
            int i21 = (int)(random.nextDouble() * MAX_ELEM1);
            int i22 = (int)(random.nextDouble() * MAX_ELEM2);
            map.add(i11, i12);
            if (map.contains(i21, i22)) {
                ++cnt;
            }
        }
        sw.stop();
        System.out.println(cnt+ ": "+sw);
    }

    @Test
    public void perf_test_bit_set() {
        BitSet map = new BitSet((MAX_ELEM1+1)*(MAX_ELEM2+1));
        int cnt = 0;
        Random random = new Random(new Date().getTime());
        StopWatch sw = new StopWatch();
        sw.start();
        for (int ii=0; ii<SAMPLE_SIZE; ++ii) {
            int i11 = (int)(random.nextDouble() * MAX_ELEM1);
            int i12 = (int)(random.nextDouble() * MAX_ELEM2);
            int i21 = (int)(random.nextDouble() * MAX_ELEM1);
            int i22 = (int)(random.nextDouble() * MAX_ELEM2);
            map.set(i11*(MAX_ELEM2+1)+i12);
            if (map.get(i21*(MAX_ELEM2+1)+i22)) {
                ++cnt;
            }
        }
        sw.stop();
        System.out.println(cnt+ ": "+sw);
    }

    @Test
    public void perf_test_array() {
        byte[][] map = new byte[MAX_ELEM1+1][MAX_ELEM2+1];
        int cnt = 0;
        Random random = new Random(new Date().getTime());
        StopWatch sw = new StopWatch();
        sw.start();
        for (int ii=0; ii<SAMPLE_SIZE; ++ii) {
            int i11 = (int)(random.nextDouble() * MAX_ELEM1);
            int i12 = (int)(random.nextDouble() * MAX_ELEM2);
            int i21 = (int)(random.nextDouble() * MAX_ELEM1);
            int i22 = (int)(random.nextDouble() * MAX_ELEM2);
            map[i11][i12] = (byte)1;
            if (map[i21][i22] != 0) {
                ++cnt;
            }
        }
        sw.stop();
        System.out.println(cnt+ ": "+sw);
    }

    @Test
    public void perf_test_IntPairHashMap() {
        IntPairHashMap<Byte> map = new IntPairHashMap<>(MAX_ELEM2, SAMPLE_SIZE);
        int cnt = 0;
        Random random = new Random(new Date().getTime());
        StopWatch sw = new StopWatch();
        sw.start();
        for (int ii=0; ii<SAMPLE_SIZE; ++ii) {
            int i11 = (int)(random.nextDouble() * MAX_ELEM1);
            int i12 = (int)(random.nextDouble() * MAX_ELEM2);
            int i21 = (int)(random.nextDouble() * MAX_ELEM1);
            int i22 = (int)(random.nextDouble() * MAX_ELEM2);
            map.put(i11, i12, (byte)1);
            if (map.containsKey(i21, i22)) {
                ++cnt;
            }
        }
        sw.stop();
        System.out.println(cnt+ ": "+sw);
    }

    @Test
    public void perf_test_HashMap_on_tuple() {
        Map<Tuple2<Integer, Integer>, Byte> cont = new HashMap<>(SAMPLE_SIZE * 3/2);
        int cnt = 0;
        Random random = new Random(new Date().getTime());
        StopWatch sw = new StopWatch();
        sw.start();
        for (int ii=0; ii<SAMPLE_SIZE; ++ii) {
            int i11 = (int)(random.nextDouble() * MAX_ELEM1);
            int i12 = (int)(random.nextDouble() * MAX_ELEM2);
            int i21 = (int)(random.nextDouble() * MAX_ELEM1);
            int i22 = (int)(random.nextDouble() * MAX_ELEM2);
            cont.put(new Tuple2<>(i11, i12), (byte)1);
            if (cont.containsKey(new Tuple2<>(i21, i22))) {
                ++cnt;
            }
        }
        sw.stop();
        System.out.println(cnt+ ": "+sw);
    }
}