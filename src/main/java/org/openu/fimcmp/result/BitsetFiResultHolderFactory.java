package org.openu.fimcmp.result;

public class BitsetFiResultHolderFactory implements FiResultHolderFactory {
    private final int totalFreqItems;
    private final int sizeEstimation;

    public BitsetFiResultHolderFactory(int totalFreqItems, int sizeEstimation) {
        this.totalFreqItems = totalFreqItems;
        this.sizeEstimation = sizeEstimation;
    }

    @Override
    public FiResultHolder newResultHolder() {
        return new BitsetFiResultHolder(totalFreqItems, sizeEstimation);
    }
}
