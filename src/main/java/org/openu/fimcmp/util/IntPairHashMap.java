package org.openu.fimcmp.util;

import scala.Tuple2;

import javax.validation.constraints.NotNull;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Faster implementation of a hash map with a key as an integer pair, based on Java HashMap. <br/>
 * Uses a single integer to represent a pair. <br/>
 * Both integers must be non-negative. <br/>
 * The second integer must always be smaller or equal to the declared max. <br/>
 * (declared max+1) * (any first value) should be less than Integer.MAX_INTEGER. <br/>
 */
public class IntPairHashMap<V> implements Map<Tuple2<Integer, Integer>, V> {
    private final int mult1;
    private final Map<Integer, V> map;

    /**
     * @param max2nd max allowed value of a second value in a pair
     * @param expElems expected number of elements in the map
     */
    public IntPairHashMap(int max2nd, int expElems) {
        Assert.isTrue(max2nd > 0);

        this.mult1 = max2nd + 1;
        //just enough capacity to store the elements:
        this.map = new HashMap<>((int)Math.ceil(expElems / 0.75f), 0.75f);
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return map.containsKey(toActKeyFromObj(key));
    }

    public boolean containsKey(int k1, int k2) {
        return map.containsKey(toActKey(k1, k2));
    }

    @Override
    public boolean containsValue(Object value) {
        //Lets hunt these usages down
        throw new UnsupportedOperationException("no efficient implementation");
    }

    @Override
    public V get(Object key) {
        return map.get(toActKeyFromObj(key));
    }

    public V get(int k1, int k2) {
        return map.get(toActKey(k1, k2));
    }

    @Override
    public V put(Tuple2<Integer, Integer> key, V value) {
        return map.put(toActKey(key), value);
    }

    public V put(int k1, int k2, V value) {
        return map.put(toActKey(k1, k2), value);
    }


    @Override
    public V remove(Object key) {
        return map.remove(toActKeyFromObj(key));
    }

    public V remove(int k1, int k2) {
        return map.remove(toActKey(k1, k2));
    }

    @Override
    public void putAll(@NotNull Map<? extends Tuple2<Integer, Integer>, ? extends V> m) {
        for (Entry<? extends Tuple2<Integer, Integer>, ? extends V> entry : m.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public Set<Tuple2<Integer, Integer>> keySet() {
        throw new UnsupportedOperationException("no efficient implementation");
    }

    @Override
    public Collection<V> values() {
        return map.values();
    }

    @Override
    public Set<Entry<Tuple2<Integer, Integer>, V>> entrySet() {
        throw new UnsupportedOperationException("no efficient implementation");
    }

    @SuppressWarnings("unchecked")
    private Integer toActKeyFromObj(Object pair) {
        return toActKey((Tuple2<Integer, Integer>)pair);
    }

    private Integer toActKey(@NotNull Tuple2<Integer, Integer> key) {
        return toActKey(key._1, key._2);
    }

    private Integer toActKey(int k1, int k2) {
        Assert.isTrue(k1 >= 0 && k2 >= 0);
        int res = k1 * mult1 + k2;
        Assert.isTrue(res >= 0);
        return res;
    }
}
