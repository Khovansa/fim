package org.openu.fimcmp.result;

public class CountingOnlyFiResultHolderFactory implements FiResultHolderFactory{
    @Override
    public FiResultHolder newResultHolder(int totalFreqItems, int sizeEstimation) {
        return new CountingOnlyFiResultHolder(totalFreqItems);
    }
}
