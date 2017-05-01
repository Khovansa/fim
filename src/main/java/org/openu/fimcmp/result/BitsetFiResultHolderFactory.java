package org.openu.fimcmp.result;

public class BitsetFiResultHolderFactory implements FiResultHolderFactory {
    @Override
    public FiResultHolder newResultHolder(int totalFreqItems, int sizeEstimation) {
        return new BitsetFiResultHolder(totalFreqItems, sizeEstimation);
    }
}
