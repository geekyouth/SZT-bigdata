package cn.java666.sztcommon.util;

public class ParseCardNo {

    private static final String MAP = "ABCDEFGHIJ";

    public static String parse(String no) {
        if (no == null || no.isEmpty()) {
            return no;
        }
        char first = no.charAt(0);
        StringBuilder sb = new StringBuilder();
        if (MAP.indexOf(first) >= 0) {
            for (char c : no.toCharArray()) {
                int idx = MAP.indexOf(c);
                if (idx >= 0) {
                    sb.append(idx);
                } else {
                    sb.append(c);
                }
            }
        } else {
            for (char c : no.toCharArray()) {
                if (Character.isDigit(c)) {
                    sb.append(MAP.charAt(c - '0'));
                } else {
                    sb.append(c);
                }
            }
        }
        return sb.toString();
    }
}
