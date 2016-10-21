package my.apriori;

import java.io.Serializable;
import java.util.*;

/**
 * Generate candidate itemsets of size i+1 from frequent itemsets of size i. <br/>
 * Necessary for next step Apriori.
 */
public class AprCandidateFisGenerator<T extends Comparable<T>> implements Serializable {
    private static final Integer[] EMPTY_COL = {};
    private static final Integer[][] EMPTY_COLS = {};

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
     * res[0] = [0, 3, 1, 6, 1, 9, 1] <br/>
     * res[1] = [3, 1, 6, 1, 9, 1] <br/>
     * res[2] = [6, 9, 1] <br/>
     * That is, the first element of each column is the first element of the pair. <br/>
     * The rest of the elements in the column are the second elements followed by their counters. <br/>
     * The counters produced by this function are always 1's, but a later reduce operation will sum those counters. <br/>
     * E.g. [3, 6, 1, 9, 1] stands for {((3, 6), 1), ((3, 9), 1)}.
     */
    //TODO: add partitioner
    Integer[][] genTransactionC2sNew(Integer[] sortedTr) {
        final int trSize = sortedTr.length;
        if (trSize <= 1) {
            return EMPTY_COLS;
        }

        Integer[][] res = new Integer[trSize-1][];
        for (int ii = 0; ii < trSize - 1; ++ii) {
            Integer[] resCol = res[ii] = new Integer[2 * (trSize - ii - 1) + 1];
            int resColInd = 0;
            resCol[resColInd++] = sortedTr[ii]; //the first element of the pair
            //Adding the second elements of the pairs (whose first element is sortedTr[ii]):
            for (int jj = ii + 1; jj < trSize; ++jj) {
                resCol[resColInd++] = sortedTr[jj];
                resCol[resColInd++] = 1;
            }
        }

        return res;
    }

    /**
     * See {@link #genTransactionC2sNew} for columns structure. <br/>
     * E.g. {0, 3, 1, 6, 1, 9, 1}. <br/>
     * That is, after the first item, we use a kind of linked list (2nd item, count),
     * i.e. a sparse representation of a map: item -> count.
     */
    Integer[] mergeC2Columns(Integer[] col1, Integer[] col2) {
        if (col1.length == 0) {
            return Arrays.copyOf(col2, col2.length);
        }

        int diffItemsCount = getDifferentItemsCountForC2s(col1, col2);
        int resLength = 1 + 2*diffItemsCount;
        if (resLength > col1.length) {
            Integer[] res = new Integer[resLength];
            mergeC2ColumnsToRes(res, col1, col2);
            return res;
        } else {
            mergeC2ColumnsToCol1(col1, col2);
            return col1;
        }
    }

    /**
     * See {@link #genTransactionC2sNew} for the column structure. <br/>
     * E.g. {0, 3, 1, 6, 1, 9, 1}. <br/>
     */
    Integer[] getC2sFilteredByMinSupport(Integer[] col, long minSuppCount) {
        int goodElemCnt = 0;
        for (int ii=2; ii<col.length; ii+=2) {
            if (col[ii] >= minSuppCount) {
                ++goodElemCnt;
            }
        }

        int resLen = 1 + 2 * goodElemCnt;
        if (resLen == col.length) {
            return col; //all pairs are frequent
        }
        if (goodElemCnt == 0) {
            //slight violation of the column structure for efficiency: it had to be {col[0]}:
            return EMPTY_COL;
        }

        Integer[] res = new Integer[resLen];
        int resInd = 0;
        res[resInd++] = col[0];
        for (int ii=2; ii<col.length; ii+=2) {
            if (col[ii] >= minSuppCount) {
                res[resInd++] = col[ii-1];  //elem
                res[resInd++] = col[ii];    //count
            }
        }
        return res;
    }

    List<Integer[]> f2ColToPairs(Integer[] col) {
        if (col.length <= 1) {
            return Collections.emptyList();
        }

        int resLen = (col.length - 1) / 2;
        List<Integer[]> res = new ArrayList<>(resLen);
        Integer item1 = col[0];
        for (int ii=1; ii<col.length; ii+=2) {
            res.add(new Integer[]{item1, col[ii], col[ii+1]});
        }
        return res;
    }
    private int getDifferentItemsCountForC2s(Integer[] col1, Integer[] col2) {
        if (col1.length == 0) {
            return col2.length;
        }

        int i1=1, i2=1; //1 since skipping over the first element of the pair
        int res = 0;
        while (i1 < col1.length && i2 < col2.length) {
            int cmp = col1[i1].compareTo(col2[i2]);
            ++res;
            if (cmp <= 0) {
                i1+=2; //2 since skipping over the item counters
            }
            if (cmp >= 0) {
                i2+=2;
            }
        }

        res += ((col1.length - i1) + (col2.length - i2))/2; //length of tails, skipping the counters
        return res;
    }

    private void mergeC2ColumnsToRes(Integer[] res, Integer[] col1, Integer[] col2) {
        res[0] = (col1.length > 0) ? col1[0] : col2[0];
        int i1=1, i2=1, ir=1;
        while (i1 < col1.length && i2 < col2.length) {
            int cmp = col1[i1].compareTo(col2[i2]);
            if (cmp < 0) {
                res[ir++] = col1[i1++]; //elem
                res[ir++] = col1[i1++]; //counter
            } else if (cmp > 0) {
                res[ir++] = col2[i2++]; //elem
                res[ir++] = col2[i2++]; //counter
            } else {
                res[ir++] = col1[i1++]; //elem
                ++i2;
                res[ir++] = col1[i1++] + col2[i2++]; //counter
            }
        }

        //tails:
        while (i1 < col1.length) {
            res[ir++] = col1[i1++]; //elem
            res[ir++] = col1[i1++]; //counter
        }
        while (i2 < col2.length) {
            res[ir++] = col2[i2++]; //elem
            res[ir++] = col2[i2++]; //counter
        }
    }

    //Assumes col2 elements set is a subset of col1's elements
    private void mergeC2ColumnsToCol1(Integer[] col1, Integer[] col2) {
        int i1=1, i2=1;
        while (i1<col1.length && i2<col2.length) {
            if (col1[i1].equals(col2[i2])) {
                col1[i1+1] += col2[i2+1]; //sum the counts
                //move both to the next element:
                i1 += 2;
                i2 += 2;
            } else {
                i1 += 2; //col2 has no such element => this element's count is unchanged
            }
        }
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
