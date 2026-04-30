package com.invest.differential.io;

import java.util.Arrays;

/** Hashable wrapper around a row's data values (handles {@code byte[]}). */
final class RowKey {
    final Object[] values;
    private final int hash;

    RowKey(Object[] values) {
        this.values = values;
        int h = 1;
        for (Object v : values) {
            h = 31 * h + valueHash(v);
        }
        this.hash = h;
    }

    @Override
    public int hashCode() { return hash; }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof RowKey other)) return false;
        if (other.hash != hash || other.values.length != values.length) return false;
        for (int i = 0; i < values.length; i++) {
            if (!valueEquals(values[i], other.values[i])) return false;
        }
        return true;
    }

    private static int valueHash(Object v) {
        if (v == null) return 0;
        if (v instanceof byte[] b) return Arrays.hashCode(b);
        return v.hashCode();
    }

    private static boolean valueEquals(Object a, Object b) {
        if (a == b) return true;
        if (a == null || b == null) return false;
        if (a instanceof byte[] ab && b instanceof byte[] bb) {
            return Arrays.equals(ab, bb);
        }
        return a.equals(b);
    }
}
