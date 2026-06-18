package com.ormuz.shared.util;

import java.io.PrintStream;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Utilitário simples de formatação de logs de terminal do ORMUZ.
 *
 * <p>Objetivo: manter os terminais dos sensores, drones, monitor e clientes
 * com linhas consistentes, legíveis e fáceis de comparar durante a auditoria.</p>
 */
public final class ConsoleLog {
    private static final DateTimeFormatter TS_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private static final DateTimeFormatter SHORT_TS_FMT = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final int COMPONENT_WIDTH = 14;
    private static final int EVENT_WIDTH = 22;
    private static final int LINE_WIDTH = 108;

    private ConsoleLog() {}

    public static void info(String component, String event, String... fields) {
        print(System.out, "INFO", component, event, fields);
    }

    public static void warn(String component, String event, String... fields) {
        print(System.out, "WARN", component, event, fields);
    }

    public static void error(String component, String event, String... fields) {
        print(System.err, "ERRO", component, event, fields);
    }

    public static void print(PrintStream out, String level, String component, String event, String... fields) {
        out.println(format(level, component, event, fields));
    }

    public static String format(String level, String component, String event, String... fields) {
        StringBuilder sb = new StringBuilder();
        sb.append(timestampNow())
          .append(" | ").append(rightPad(safe(level), 5))
          .append(" | ").append(rightPad(safe(component), COMPONENT_WIDTH))
          .append(" | ").append(rightPad(safe(event), EVENT_WIDTH));

        if (fields != null) {
            for (int i = 0; i < fields.length; i += 2) {
                String key = safe(fields[i]);
                String value = (i + 1 < fields.length) ? safe(fields[i + 1]) : "";
                sb.append(" | ").append(key).append("=").append(value);
            }
        }
        return sb.toString();
    }

    public static void section(String title) {
        section(System.out, title);
    }

    public static void section(PrintStream out, String title) {
        out.println();
        out.println("═".repeat(LINE_WIDTH));
        out.println(center(" " + safe(title) + " ", LINE_WIDTH));
        out.println("═".repeat(LINE_WIDTH));
    }

    public static void subsection(String title) {
        System.out.println();
        System.out.println("─".repeat(LINE_WIDTH));
        System.out.println("  " + safe(title));
        System.out.println("─".repeat(LINE_WIDTH));
    }

    public static String row(String left, String right) {
        return "  " + rightPad(safe(left), 28) + " : " + safe(right);
    }

    public static String timestampNow() {
        return LocalDateTime.now().format(TS_FMT);
    }

    public static String shortTimestampNow() {
        return LocalDateTime.now().format(SHORT_TS_FMT);
    }

    public static String formatEpochMillis(long epochMillis) {
        if (epochMillis <= 0) return "GENESIS";
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneId.systemDefault()).format(TS_FMT);
    }

    public static String shortHash(String value) {
        return shortText(value, 12);
    }

    public static String shortText(String value, int maxLen) {
        if (value == null || value.isBlank()) return "N/A";
        if (maxLen < 4) maxLen = 4;
        return value.length() <= maxLen ? value : value.substring(0, maxLen) + "...";
    }

    public static String safe(Object value) {
        if (value == null) return "N/A";
        String text = String.valueOf(value).replace('\n', ' ').replace('\r', ' ').trim();
        return text.isBlank() ? "N/A" : text;
    }

    public static String rightPad(String text, int width) {
        String safeText = safe(text);
        if (safeText.length() >= width) return safeText;
        return safeText + " ".repeat(width - safeText.length());
    }

    public static String leftPad(String text, int width) {
        String safeText = safe(text);
        if (safeText.length() >= width) return safeText;
        return " ".repeat(width - safeText.length()) + safeText;
    }

    public static String center(String text, int width) {
        String safeText = safe(text);
        if (safeText.length() >= width) return safeText;
        int left = (width - safeText.length()) / 2;
        int right = width - safeText.length() - left;
        return " ".repeat(left) + safeText + " ".repeat(right);
    }
}
