package org.openu.fimcmp.util;

/**
 * Reduced copy-paste of the Spring Assert.
 */
public class Assert {
    public static void isTrue(boolean cond, String msg) {
        if (!cond) {
            throw new IllegalArgumentException(msg);
        }
    }

    public static void isTrue(boolean cond) {
        if (!cond) {
            throw new IllegalArgumentException();
        }
    }
}
