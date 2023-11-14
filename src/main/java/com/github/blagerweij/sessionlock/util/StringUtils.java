package com.github.blagerweij.sessionlock.util;

import java.util.Locale;

public class StringUtils {

    StringUtils() {
        throw new IllegalArgumentException("Utility class");
    }

    public static String truncate(final String str, final int maxlength) {
        if (str == null || str.length() <= maxlength) {
            return str;
        } else {
            return str.substring(0, maxlength);
        }
    }

    public static String toUpperCase(final String str) {
        if (str == null) {
            return null;
        } else {
            return str.toUpperCase(Locale.ROOT);
        }
    }
}
