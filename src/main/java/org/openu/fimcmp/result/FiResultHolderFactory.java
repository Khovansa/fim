package org.openu.fimcmp.result;

import java.io.Serializable;

public interface FiResultHolderFactory extends Serializable {
    FiResultHolder newResultHolder();
}
