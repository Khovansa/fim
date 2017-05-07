package org.openu.fimcmp.result;

public class CountingOnlyFiResultHolderFactory implements FiResultHolderFactory{
    private final int totalFreqItems;

    public CountingOnlyFiResultHolderFactory(int totalFreqItems) {
        this.totalFreqItems = totalFreqItems;
    }

    @Override
    public FiResultHolder newResultHolder() {
        return new CountingOnlyFiResultHolder(totalFreqItems);
    }
}
