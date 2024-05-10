package com.flink.messenger.utils;

public class FlinkHelper {

    public static String[] splitIntoTwoParts(String input) {
        String[] parts = input.split("\\s+", 2);
        if (parts.length == 2) {
            return parts;
        } else {
            return new String[]{parts[0], ""};
        }
    }
}
