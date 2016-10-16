package my.apriori;

import java.io.Serializable;
import java.util.*;

/**
 * Generate candidate itemsets of size i+1 from frequent itemsets of size i. <br/>
 * Necessary for next step Apriori.
 */
public class AprCandidateFisGenerator<T extends Comparable<T>> implements Serializable {

    Collection<List<T>> getNextSizeCandItemsets(Collection<List<T>> oldFis) {
        if (oldFis.isEmpty()) {
            return Collections.emptyList();
        }

        List<T> sortedSeedItems = pickAndSortItems(oldFis, null);
        return getNextSizeItemsetsByCross(oldFis, sortedSeedItems, null);
    }

    Collection<List<T>> getNextSizeCandItemsetsFromTransaction(
            Collection<List<T>> trAsCandidatesOfSizeK, int k, Set<T> f1, Set<List<T>> oldFisOfSizeK) {
        if (oldFisOfSizeK.size() <= k || trAsCandidatesOfSizeK.size() <= k) {
            return Collections.emptyList();
        }

        List<T> sortedSeedItems = pickAndSortItems(trAsCandidatesOfSizeK, f1);
        return getNextSizeItemsetsByCross(trAsCandidatesOfSizeK, sortedSeedItems, oldFisOfSizeK);
    }

    Collection<List<T>> genTransactionC2s(ArrayList<T> sortedTr) {
        final int trSize = sortedTr.size();
        if (trSize <= 1) {
            return Collections.emptyList();
        }

        ArrayList<List<T>> res = new ArrayList<>(trSize * (trSize - 1) / 2);
        for (int ii = 0; ii < trSize; ++ii) {
            T item1 = sortedTr.get(ii);
            for (int jj = ii + 1; jj < trSize; ++jj) {
                T item2 = sortedTr.get(jj);
                res.add(Arrays.asList(item1, item2));
            }
        }

        res.trimToSize();
        return res;
    }

    /**
     * Pairs {(0, 3), (0, 6), (0, 9), (3, 6), (3, 9), (6, 9)} will be held as followed: <br/>
     * res[0] = [0, 3, 6, 9] <br/>
     * res[1] = [3, 6, 9] <br/>
     * res[2] = [6, 9] <br/>
     * That is, the first element of each column is the first element of the pair. <br/>
     * The rest of the elements in the column are the second elements. <br/>
     * E.g. [3, 6, 9] stands for {(3, 6), (3, 9)}.
     */
    //TODO: add partitioner
    Integer[][] genTransactionC2sNew(Integer[] sortedTr) {
        final int trSize = sortedTr.length;
        if (trSize <= 1) {
            return new Integer[0][];
        }

        Integer[][] res = new Integer[trSize-1][];
        for (int ii = 0; ii < trSize - 1; ++ii) {
            Integer[] resCol = res[ii] = new Integer[trSize - ii];
            int resColInd = 0;
            resCol[resColInd++] = sortedTr[ii]; //the first element of the pair
            //Adding the second elements of the pairs (whose first element is sortedTr[ii]):
            for (int jj = ii + 1; jj < trSize; ++jj) {
                resCol[resColInd++] = sortedTr[jj];
            }
        }

        return res;
    }

    private List<List<T>> getNextSizeItemsetsByCross(
            Collection<List<T>> seedItemsetsOfSizeK, List<T> sortedSeedItems, Set<List<T>> oldFisOfSizeK) {
        final int estCapacity = seedItemsetsOfSizeK.size() * 10;
        ArrayList<List<T>> resKp1 = new ArrayList<>(estCapacity);

        if (oldFisOfSizeK == null) {
            oldFisOfSizeK = new HashSet<>(seedItemsetsOfSizeK);//just taking the existing seeds
        }

        for (List<T> seedK : seedItemsetsOfSizeK) {
            addNextSizeItemsets(resKp1, seedK, sortedSeedItems, oldFisOfSizeK);
        }

        resKp1.trimToSize();
        return resKp1;
    }

    private void addNextSizeItemsets(
            List<List<T>> resKp1,
            List<T> seedK, List<T> sortedSeedItems, Set<List<T>> oldFisOfSizeK) {
        //infrequent k-itemset can't produce frequent (k+1)-itemset
        if (!optSetContains(oldFisOfSizeK, seedK)) {
            return;
        }

        T firstInSeedK = seedK.get(0);
        for (T seedItem : sortedSeedItems) {
            if (seedItem.compareTo(firstInSeedK) >= 0) {
                //we only create ({seedItem} U seedK) if seedItem < seedK[0]
                break;
            }

            ArrayList<T> newCandKp1 = newCandKp1IfPossible(seedK, seedItem, oldFisOfSizeK);
            if (newCandKp1 != null) {
                resKp1.add(newCandKp1);
            }
        }
    }

    private ArrayList<T> newCandKp1IfPossible(
            List<T> seedK, T seedItem, Set<List<T>> oldFisOfSizeK) {

        for (int ii = 0; ii < seedK.size(); ++ii) {
            List<T> fiK = withSeedItemInsteadOf(seedK, seedItem, ii);//assuming seedItem < seedK[0]
            if (!oldFisOfSizeK.contains(fiK)) {
                //No chance that ({seedItem} U seedK) will be frequent since even its subset is not frequent
                return null;
            }
        }

        return asList(seedItem, seedK); //sorted since assuming seedItem < seedK[0]
    }

    //returns a sorted list assuming seedItem < seedK[0]
    private List<T> withSeedItemInsteadOf(List<T> seedK, T seedItem, int ii) {
        final int k = seedK.size();
        List<T> res = new ArrayList<>(k);
        res.add(seedItem);
        res.addAll(seedK.subList(0, ii));
        res.addAll(seedK.subList(ii + 1, k));
        return res;
    }

    private static <T> List<T> pickAndSortItems(Collection<List<T>> inFis, Set<T> f1) {
        SortedSet<T> res = new TreeSet<>();
        for (Collection<T> is : inFis) {
            for (T item : is) {
                if (optSetContains(f1, item)) {
                    res.add(item);
                }
            }
        }
        return new ArrayList<>(res);
    }

    private static <V> boolean optSetContains(Set<V> optSet, V val) {
        return optSet == null || optSet.contains(val);
    }

    private static <T> ArrayList<T> asList(T seedItem, Collection<T> seedK) {
        ArrayList<T> res = new ArrayList<>(seedK.size() + 1);
        res.add(seedItem);
        res.addAll(seedK);
        return res;
    }
}
