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
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");
    private static final DateTimeFormatter TIME_FORMATTER =
            DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSSSSS");

    private static final Map<Character, String> ESCAPE_MAPPING;

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
        ESCAPE_MAPPING = Collections.unmodifiableMap(map);
    }

    private JdbcValueFormatter() {
    }

    private static String formatInt(int myInt) {
        return Integer.toString(myInt);
    }

    private static String formatFloat(float myFloat) {
        return Float.toString(myFloat);
    }

    private static String formatDouble(double myDouble) {
        return Double.toString(myDouble);
    }

    private static String formatLong(long myLong) {
        return Long.toString(myLong);
    }

    private static String formatBigDecimal(BigDecimal myBigDecimal) {
        return myBigDecimal != null ? myBigDecimal.toPlainString() : null;
    }

    private static String formatShort(short myShort) {
        return Short.toString(myShort);
    }

    private static String formatString(String myString) {
        return escape(myString);
    }

    private static String formatBoolean(boolean myBoolean) {
        return myBoolean ? "1" : "0";
    }

    private static String formatLocalDate(LocalDate x) {
        System.out.println("call formatLocalDate");
        return DATE_FORMATTER.format(x);
    }

    private static String formatLocalDateTime(LocalDateTime x) {
        System.out.println("call formatLocalDateTime");
        return DATE_TIME_FORMATTER.format(x);
    }

    private static String formatLocalTime(LocalTime x) {
        System.out.println("call formatLocalTime");
        return TIME_FORMATTER.format(x);
    }


    private static String formatDate(Date date) {
//        return DATE_FORMATTER.format(date);
        return null;
    }

    private static String formatTime(Time time) {
        return TIME_FORMATTER.format(
                Instant.ofEpochMilli(time.getTime()));
    }

    private static String formatTimestamp(Timestamp time) {
//        System.out.println("call formatTimestamp");
//        SimpleDateFormat formatter = getDateTimeFormat();
//        formatter.setTimeZone(timeZone);
//        StringBuilder formatted = new StringBuilder(formatter.format(time));
//        if (time != null && time.getNanos() % 1000000 > 0) {
//            formatted.append('.').append(time.getNanos());
//        }
//        return formatted.toString();
        return null;
    }

    private static String formatBigInteger(BigInteger x) {
        return x.toString();
    }

    public static String formatObject(Object x) {
        TimeZone timeZone = getTimeZone();

        String value = formatObject(x, timeZone, timeZone);

        if (value == null) {
            return null;
        } else {
            return needsQuoting(x) ? String.join("", "'", value, "'") : value;
        }

    }

    private static String formatObject(Object x, TimeZone dateTimeZone,
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
            return formatDate((Date) x);
        } else if (x instanceof Time) {
            return formatTime((Time) x);
        } else if (x instanceof Timestamp) {
            return formatTimestamp((Timestamp) x);
        } else if (x instanceof LocalTime) {
            return formatLocalTime((LocalTime) x);
        } else if (x instanceof LocalDate) {
            return formatLocalDate((LocalDate) x);
        } else if (x instanceof LocalDateTime) {
            return formatLocalDateTime((LocalDateTime) x);
        } else if (x instanceof Boolean) {
            return formatBoolean((Boolean) x);
        } else if (x instanceof BigInteger) {
            return formatBigInteger((BigInteger) x);
        } else {
            return null;
        }
    }

    private static boolean needsQuoting(Object o) {
        return o != null
                && !(o instanceof Array)
                && !(o instanceof Boolean)
                && !(o instanceof Collection)
                && !(o instanceof Map)
                && !(o instanceof Number)
                && !o.getClass().isArray();
    }

    private static TimeZone getTimeZone() {
        return TimeZone.getDefault();
    }

    private static String escape(String s) {
        if (s == null) {
            return "\\N";
        }

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);

            String escaped = ESCAPE_MAPPING.get(ch);
            if (escaped != null) {
                sb.append(escaped);
            } else {
                sb.append(ch);
            }
        }

        return sb.toString();
    }
}
