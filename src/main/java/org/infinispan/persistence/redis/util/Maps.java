package org.infinispan.persistence.redis.util;

public final class Maps {
    public static final int MAX_POWER_OF_TWO = 1 << (Integer.SIZE - 2);

    private Maps() {}

    static int checkNonnegative(int value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " cannot be negative but was: " + value);
        }
        return value;
    }

    public static int capacity(int expectedSize) {
        if (expectedSize < 3) {
            checkNonnegative(expectedSize, "expectedSize");
            return expectedSize + 1;
        }
        if (expectedSize < MAX_POWER_OF_TWO) {
            return (int) Math.ceil(expectedSize / 0.75);
        }
        return Integer.MAX_VALUE;
    }
}
