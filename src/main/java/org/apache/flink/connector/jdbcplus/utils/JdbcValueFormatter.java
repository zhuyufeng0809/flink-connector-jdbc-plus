package org.apache.flink.connector.jdbcplus.utils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Array;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2022-06-22
 * @Description:
 */
public class JdbcValueFormatter {

    private static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter TIME_FORMATTER =
            DateTimeFormatter.ofPattern("HH:mm:ss");

    private static final Map<Character, String> escapeMapping;

    static {
        Map<Character, String> map = new HashMap<>();
        map.put('\\', "\\\\");
        map.put('\n', "\\n");
        map.put('\t', "\\t");
        map.put('\b', "\\b");
        map.put('\f', "\\f");
        map.put('\r', "\\r");
        map.put('\0', "\\0");
        map.put('\'', "\\'");
        map.put('`', "\\`");
        escapeMapping = Collections.unmodifiableMap(map);
    }

    private static final ThreadLocal<SimpleDateFormat> dateFormat = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd"));

    private static final ThreadLocal<SimpleDateFormat> dateTimeFormat = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));

    public static String formatInt(int myInt) {
        return Integer.toString(myInt);
    }

    public static String formatDouble(double myDouble) {
        return Double.toString(myDouble);
    }

    public static String formatLong(long myLong) {
        return Long.toString(myLong);
    }

    public static String formatFloat(float myFloat) {
        return Float.toString(myFloat);
    }

    public static String formatBigDecimal(BigDecimal myBigDecimal) {
        return myBigDecimal != null ? myBigDecimal.toPlainString() : null;
    }

    public static String formatShort(short myShort) {
        return Short.toString(myShort);
    }

    public static String formatString(String myString) {
        return escape(myString);
    }

    public static String formatBoolean(boolean myBoolean) {
        return myBoolean ? "1" : "0";
    }

    public static String formatDate(Date date, TimeZone timeZone) {
        SimpleDateFormat formatter = getDateFormat();
        formatter.setTimeZone(timeZone);
        return formatter.format(date);
    }

    public static String formatTime(Time time, TimeZone timeZone) {
        return TIME_FORMATTER.format(
                Instant.ofEpochMilli(time.getTime())
                        .atZone(timeZone.toZoneId())
                        .toLocalTime());
    }

    public static String formatTimestamp(Timestamp time, TimeZone timeZone) {
        SimpleDateFormat formatter = getDateTimeFormat();
        formatter.setTimeZone(timeZone);
        StringBuilder formatted = new StringBuilder(formatter.format(time));
        if (time != null && time.getNanos() % 1000000 > 0) {
            formatted.append('.').append(time.getNanos());
        }
        return formatted.toString();
    }

    public static String formatBigInteger(BigInteger x) {
        return x.toString();
    }

    public static String formatLocalDate(LocalDate x) {
        return DATE_FORMATTER.format(x);
    }

    public static String formatLocalDateTime(LocalDateTime x) {
        return DATE_TIME_FORMATTER.format(x);
    }

    public static String formatLocalTime(LocalTime x) {
        return TIME_FORMATTER.format(x);
    }

    public static String formatOffsetTime(OffsetTime x) {
        return DateTimeFormatter.ISO_OFFSET_TIME.format(x);
    }

    public static String formatOffsetDateTime(OffsetDateTime x, TimeZone timeZone) {
        return DATE_TIME_FORMATTER
                .withZone(timeZone.toZoneId())
                .format(x);
    }

    public static String formatZonedDateTime(ZonedDateTime x, TimeZone timeZone) {
        return DATE_TIME_FORMATTER
                .withZone(timeZone.toZoneId())
                .format(x);
    }

    public static String formatInstant(Instant x, TimeZone timeZone) {
        return DATE_TIME_FORMATTER
                .withZone(timeZone.toZoneId())
                .format(x);
    }

    public static String formatObject(Object x, TimeZone dateTimeZone,
                                      TimeZone dateTimeTimeZone)
    {
        if (x instanceof String) {
            return formatString((String) x);
        } else if (x instanceof BigDecimal) {
            return formatBigDecimal((BigDecimal) x);
        } else if (x instanceof Short) {
            return formatShort((Short) x);
        } else if (x instanceof Integer) {
            return formatInt((Integer) x);
        } else if (x instanceof Long) {
            return formatLong((Long) x);
        } else if (x instanceof Float) {
            return formatFloat((Float) x);
        } else if (x instanceof Double) {
            return formatDouble((Double) x);
        } else if (x instanceof Date) {
            return formatDate((Date) x, dateTimeZone);
        } else if (x instanceof LocalDate) {
            return formatLocalDate((LocalDate) x);
        } else if (x instanceof Time) {
            return formatTime((Time) x, dateTimeTimeZone);
        } else if (x instanceof LocalTime) {
            return formatLocalTime((LocalTime) x);
        } else if (x instanceof Instant) {
            return formatInstant((Instant) x, dateTimeZone);
        } else if (x instanceof OffsetTime) {
            return formatOffsetTime((OffsetTime) x);
        } else if (x instanceof Timestamp) {
            return formatTimestamp((Timestamp) x, dateTimeTimeZone);
        } else if (x instanceof LocalDateTime) {
            return formatLocalDateTime((LocalDateTime) x);
        } else if (x instanceof OffsetDateTime) {
            return formatOffsetDateTime((OffsetDateTime) x, dateTimeTimeZone);
        } else if (x instanceof ZonedDateTime) {
            return formatZonedDateTime((ZonedDateTime) x, dateTimeTimeZone);
        } else if (x instanceof Boolean) {
            return formatBoolean((Boolean) x);
        } else if (x instanceof BigInteger) {
            return formatBigInteger((BigInteger) x);
        } else {
            return String.valueOf(x);
        }
    }

    public static boolean needsQuoting(Object o) {
        return o != null
                && !(o instanceof Array)
                && !(o instanceof Boolean)
                && !(o instanceof Collection)
                && !(o instanceof Map)
                && !(o instanceof Number)
                && !o.getClass().isArray();
    }

    private static SimpleDateFormat getDateFormat() {
        return dateFormat.get();
    }

    private static SimpleDateFormat getDateTimeFormat() {
        return dateTimeFormat.get();
    }

    public static String escape(String s) {
        if (s == null) {
            return "\\N";
        }

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);

            String escaped = escapeMapping.get(ch);
            if (escaped != null) {
                sb.append(escaped);
            } else {
                sb.append(ch);
            }
        }

        return sb.toString();
    }
}
