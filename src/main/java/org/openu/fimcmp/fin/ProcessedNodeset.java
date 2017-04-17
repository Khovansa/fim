package org.openu.fimcmp.fin;

import org.apache.commons.collections.CollectionUtils;
import org.openu.fimcmp.result.FiResultHolder;
import org.openu.fimcmp.util.Assert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 *
 */
class ProcessedNodeset {
    private final DiffNodeset diffNodeset;
    private ArrayList<Integer> equivalentItems = null;
    private LinkedList<DiffNodeset> sons = null;

    ProcessedNodeset(DiffNodeset diffNodeset) {
        Assert.isTrue(diffNodeset != null);
        this.diffNodeset = diffNodeset;
    }

    void updateResult(FiResultHolder resultHolder, List<Integer> parentEquivItems) {
        resultHolder.addClosedItemset(
                diffNodeset.getSupportCnt(), diffNodeset.getItemset(), parentEquivItems, equivalentItems);
    }

    List<ProcessedNodeset> processSonsOnly(FiResultHolder resultHolder, int minSuppCnt) {
        if (CollectionUtils.isEmpty(sons)) {
            return Collections.emptyList();
        }

        List<ProcessedNodeset> res = new ArrayList<>(sons.size());
        while (!sons.isEmpty()) {
            DiffNodeset son = sons.pop();
            ProcessedNodeset processedSon = son.createEquivItemsAndSons(false, sons, minSuppCnt);
            processedSon.updateResult(resultHolder, equivalentItems);
            res.add(processedSon);
        }

        return res;
    }

    void processSubtree(FiResultHolder resultHolder, int minSuppCnt) {
        if (CollectionUtils.isEmpty(sons)) {
            return;
        }

        while (!sons.isEmpty()) {
            DiffNodeset son = sons.pop();
            ProcessedNodeset processedSon = son.createEquivItemsAndSons(false, sons, minSuppCnt);
            processedSon.updateResult(resultHolder, equivalentItems);
            processedSon.processSubtree(resultHolder, minSuppCnt);
        }
    }

    List<DiffNodeset> getSons() {
        if (sons != null) {
            return sons;
        } else {
            return Collections.emptyList();
        }
    }

    void addNewEquivItem(int item) {
        if (equivalentItems == null) {
            equivalentItems = new ArrayList<>(2);
        }
        equivalentItems.add(item);
    }

    void addSon(DiffNodeset son) {
        if (sons == null) {
            sons = new LinkedList<>();
        }
        sons.add(son);
    }
}
