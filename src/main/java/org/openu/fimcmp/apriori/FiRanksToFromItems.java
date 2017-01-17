package org.openu.fimcmp.apriori;

import org.apache.commons.lang.ArrayUtils;
import org.openu.fimcmp.util.Assert;

import java.io.Serializable;
import java.util.*;

/**
 * Map any FI rank to and from the actual itemset (i.e. list of items)
 */
public class FiRanksToFromItems implements Serializable {
    private final ArrayList<CurrSizeFiRanks> fiRanksKto2;
    private final int maxK;

    public FiRanksToFromItems(CurrSizeFiRanks... fiRanksKto2) {
        this(Arrays.asList(fiRanksKto2));
    }

    private FiRanksToFromItems(List<CurrSizeFiRanks> fiRanksKto2) {
        this.fiRanksKto2 = new ArrayList<>(fiRanksKto2);
        this.maxK = 1 + fiRanksKto2.size(); //rank mappers start from 2, but k starts from 1
    }

    public FiRanksToFromItems toNextSize(CurrSizeFiRanks nextSizeFiRanks) {
        List<CurrSizeFiRanks> newFiRanksList = new ArrayList<>(1 + fiRanksKto2.size());
        newFiRanksList.add(nextSizeFiRanks);
        newFiRanksList.addAll(fiRanksKto2);
        return new FiRanksToFromItems(newFiRanksList);
    }

    int getMaxK() {
        return maxK;
    }

    List<List<String>> toOrigItemsetsForDebug(List<long[]> tidMergeSets, int k, String[] r1ToItem, int maxItemsets) {
        maxItemsets = Math.min(maxItemsets, tidMergeSets.size());
        List<List<String>> res = new ArrayList<>(maxItemsets+1);
        res.add(Collections.singletonList(String.format("Total=%s", tidMergeSets.size())));
        for (long[] tidMergeSet : tidMergeSets.subList(0, maxItemsets)) {
            res.add(getOrigItemsetByRank((int)tidMergeSet[0], k, r1ToItem));
        }
        return res;
    }

    List<String> getOrigItemsetByRank(int rankK, int k, String[] r1ToItem) {
        List<String> res = new ArrayList<>(maxK);
        addToResultOrigItemsetByRank(res, rankK, k, r1ToItem);
        return res;
    }

    /**
     * A new, larger-size itemset (newItem, rankMaxK) has a chance to be frequent only if all its subsets of size maxK
     * are frequent. <br/>
     * 'rankMaxK' already designates a frequent itemset of size maxK, say {i1, ..., ik} <br/>
     * Now we need to check whether all other maxK-sized subsets of {newItem, i1, ..., ik} are frequent. <br/>
     * We do the checks taking into account that 'fiRanksKto2' is a list of mappers, <br/>
     * first of which is able to replace 'rankMaxK' by pair (i1, rank{maxK-1}), <br/>
     * second to replace rank{maxK-1} by pair (i2, rank{maxK-2}), and the last able to replace rank2 by (i{k-1}, ik).<br/>
     * Note that to avoid duplicates we require newItem < i1 and assume i1 < i2 < ... ik. <br/>
     */
    boolean couldBeFrequentAndOrdered(int newItem, int rankMaxK) {
        if (fiRanksKto2.isEmpty()) {
            return true; //maxK=1: no smaller-size itemsets yet
        }

        CurrSizeFiRanks maxKFiRanks = fiRanksKto2.get(0); //mapper to replace 'rankMaxK' by pair (i1, rank{maxK-1})
        int[] itemAndRkm1 = maxKFiRanks.getCurrSizeFiAsPairByRank(rankMaxK);
        int i1 = itemAndRkm1[0];
        if (newItem >= i1) {
            return false; //ordering: should be newItem < i1 < i2 < ...
        }

        int[] prefixItems = new int[maxK];
        prefixItems[0] = newItem;
        return couldBeFrequent(prefixItems, 1, rankMaxK);
    }

    /**
     * Check thak all k-size subsets of S={newItem, i1, ..., ik} are frequent. <br/>
     * {i1, ..., ik} is assumed to be frequent and not checked. <br/>
     * Initially, pref=[newItem], k = maxK, rankK designates {i1, ...ik}. <br/>
     * The check will be for {newItem, i2...ik}. <br/>
     * Next, pref = {newItem, i1}, k=maxK-1, rankK designates {i2...ik}. <br/>
     * The check will be for {newItem, i1, i3...ik}. <br/>
     * Etc. <br/>
     * So the order of execution is to first check S-{i1}, then S-{i2}, ... eventually S-{ik}. <br/>
     * We'll create the subsets by dropping i1 from S, then reqursively dropping i2 etc. <br/>
     */
    private boolean couldBeFrequent(int[] prefixItems, int prefSize, int rankK) {
        if (prefSize > fiRanksKto2.size()) {
            return existsItemset(prefixItems, prefSize); //<--- recursion stop, k=1, rankK=ik, i.e. checking S - {ik}
        }

        CurrSizeFiRanks kFiRanks = fiRanksKto2.get(prefSize - 1);
        int[] itemAndRkm1 = kFiRanks.getCurrSizeFiAsPairByRank(rankK);
        int i1 = itemAndRkm1[0];
        int rankKm1 = itemAndRkm1[1];
        final int km1 = maxK - prefSize;
        //Check S-{i1} = {newItem, i2, ..., ik}:
        if (!existsItemset(prefixItems, prefSize, rankKm1, km1)) {
            return false;
        }

        prefixItems[prefSize++] = i1; //the rest of the subsets will include {newItem, i1}
        return couldBeFrequent(prefixItems, prefSize, rankKm1); //<-- recursion
    }

    void addToResultOrigItemsetByRank(List<String> res, int rankK, int k, String[] r1ToItem) {
        int[] itemsetAsR1s = getItemsetByRank(rankK, k);

        for (int r1 : itemsetAsR1s) {
            res.add(r1ToItem[r1]);
        }
    }

    int[] getItemsetByRankMaxK(int rankK) {
        return getItemsetByRank(rankK, maxK);
    }

    int[] getItemsetByRank(int rankK, int k) {
        Assert.isTrue(k <= maxK && k >= 1);

        List<Integer> res = new ArrayList<>(k);
        int currRank = rankK;
        Iterator<CurrSizeFiRanks> fiRanksIt = fiRanksKto2.listIterator(maxK - k);
        while (fiRanksIt.hasNext()) {
            CurrSizeFiRanks fiRanks = fiRanksIt.next();
            int[] pair = fiRanks.getCurrSizeFiAsPairByRank(currRank);
            //pair matching rankK = (item, rank<i-1>):
            res.add(pair[0]);
            currRank = pair[1]; //(i-1)-FI rank
        }
        res.add(currRank); //should be an item

        Assert.isTrue(res.size() == k);
        return ArrayUtils.toPrimitive(res.toArray(new Integer[k]));
    }

    public PairRanks constructRkToRkm1AndR1ForMaxK() {
        return constructRkToRkm1AndR1(maxK);
    }

    private PairRanks constructRkToRkm1AndR1(int k) {
        Assert.isTrue(k <= maxK && k > 1);

        final CurrSizeFiRanks fiRanksK = fiRanksKto2.get(maxK - k);
        final int totalRanksK = fiRanksK.getTotalCurrSizeRanks();
        int[][] rkToRkm1AndR1 = new int[totalRanksK][];
        for (int rankK=0; rankK < totalRanksK; ++rankK) {
            int[] itemsetK = getItemsetByRank(rankK, k);
            Assert.isTrue(itemsetK.length == k);
            int rKm1 = getRankByItemsetIfExists(itemsetK, itemsetK.length - 1);
            Assert.isTrue(rKm1 >= 0);
            int r1 = itemsetK[itemsetK.length - 1];
            rkToRkm1AndR1[rankK] = new int[]{rKm1, r1};
        }

        final int totalRkm1s = fiRanksK.getTotalPrevSizeRanks();
        final int totalR1s = fiRanksK.getTotalFreqItems();
        return PairRanks.constructByRankToPair(rkToRkm1AndR1, totalRkm1s, totalR1s);
    }

    private boolean existsItemset(int[] itemset, int itemsetLen) {
        return getRankByItemsetIfExists(itemset, itemsetLen) >= 0;
    }

    private int getRankByItemsetIfExists(int[] itemset, int itemsetLen) {
        Assert.isTrue(itemsetLen > 0);
        return getRankByItemsetIfExists(itemset, itemsetLen - 1, itemset[itemsetLen-1], 1);
    }

    private boolean existsItemset(int[] itemset, int itemsetLen, int rankK, int kk) {
        return getRankByItemsetIfExists(itemset, itemsetLen, rankK, kk) >= 0;
    }

    /**
     * The main idea of this method: <br/>
     * ([i1, i2, i3, i4], null) -> ([i1, i2, i3], r1=i4) -> ([i1, i2], r2=~[i3, i4]) ->
     * ([i1], r3=~[i2, i3, i4]) -> ([], r4=~[i1, i2, i3, i4]) <br/>
     * In other words, replace the pair ('pref' last item, rankK) by rank{k+1}
     * and repeat this process until 'pref' becomes empty.
     */
    private int getRankByItemsetIfExists(int[] pref, int prefSize, int rankK, int kk) {
        Assert.isTrue(kk + prefSize <= maxK);

        final int totalMappers = fiRanksKto2.size();
        while (prefSize > 0) {
            int lastPrefItem = pref[prefSize - 1];
            //e.g. ([i1, i2, i3], rank1) -> ([i1, i2], rank2) -> ([i1], rank3), that's the whole idea
            CurrSizeFiRanks mapperKp1 = fiRanksKto2.get(totalMappers - kk);

            ++kk;
            rankK = mapperKp1.getCurrSizeFiRankByPair(lastPrefItem, rankK);
            if (rankK < 0) {
                break; //<--- no such frequent itemset
            }
            --prefSize;
        }

        return rankK;
    }
}
