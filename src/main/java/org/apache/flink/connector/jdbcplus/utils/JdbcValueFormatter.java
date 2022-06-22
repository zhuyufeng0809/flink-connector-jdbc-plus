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

    public static final String NULL_MARKER  = "\\N";

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

    public static String formatBytes(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        char[] hexArray =
                {'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'};
        char[] hexChars = new char[bytes.length * 4];
        int v;
        for ( int j = 0; j < bytes.length; j++ ) {
            v = bytes[j] & 0xFF;
            hexChars[j * 4]     = '\\';
            hexChars[j * 4 + 1] = 'x';
            hexChars[j * 4 + 2] = hexArray[v/16];
            hexChars[j * 4 + 3] = hexArray[v%16];
        }
        return new String(hexChars);
    }

    public static String formatInt(int myInt) {
        return Integer.toString(myInt);
    }

    public static String formatDouble(double myDouble) {
        return Double.toString(myDouble);
    }

    public static String formatChar(char myChar) {
        return Character.toString(myChar);
    }

    public static String formatLong(long myLong) {
        return Long.toString(myLong);
    }

    public static String formatFloat(float myFloat) {
        return Float.toString(myFloat);
    }

    public static String formatBigDecimal(BigDecimal myBigDecimal) {
        return myBigDecimal != null ? myBigDecimal.toPlainString() : NULL_MARKER;
    }

    public static String formatShort(short myShort) {
        return Short.toString(myShort);
    }

    public static String formatString(String myString) {
        return escape(myString);
    }

    public static String formatNull() {
        return NULL_MARKER;
    }

    public static String formatByte(byte myByte) {
        return Byte.toString(myByte);
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
        // TODO implement a true prepared statement to format according to parameter type
        if (time != null && time.getNanos() % 1000000 > 0) {
            formatted.append('.').append(time.getNanos());
        }
        return formatted.toString();
    }

    public static String formatUUID(UUID x) {
        return x.toString();
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

    public static String formatMap(Map<?, ?> map, TimeZone dateTimeZone, TimeZone dateTimeTimeZone) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<?, ?> e : map.entrySet()) {
            Object key = e.getKey();
            Object value = e.getValue();
            sb.append(',');

            if (key instanceof String) {
                sb.append('\'').append(formatString((String) key)).append('\'');
            } else {
                sb.append(formatObject(key, dateTimeZone, dateTimeTimeZone));
            }

            sb.append(':');

            if (value instanceof String) {
                sb.append('\'').append(formatString((String) value)).append('\'');
            } else {
                sb.append(formatObject(value, dateTimeZone, dateTimeTimeZone));
            }
        }
        if (sb.length() > 0) {
            sb.deleteCharAt(0);
        }
        return sb.insert(0, '{').append('}').toString();
    }

    public static String formatObject(Object x, TimeZone dateTimeZone,
                                      TimeZone dateTimeTimeZone)
    {
        if (x == null) {
            return null;
        }
        if (x instanceof Byte) {
            return formatInt(((Byte) x).intValue());
        } else if (x instanceof String) {
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
        } else if (x instanceof byte[]) {
            return formatBytes((byte[]) x);
        } else if (x instanceof Date) {
            return formatDate((Date) x, dateTimeZone);
        } else if (x instanceof LocalDate) {
            return formatLocalDate((LocalDate) x);
        } else if (x instanceof Time) {
            return formatTime((Time) x, dateTimeTimeZone);
        } else if (x instanceof LocalTime) {
            return formatLocalTime((LocalTime) x);
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
        } else if (x instanceof UUID) {
            return formatUUID((UUID) x);
        } else if (x instanceof BigInteger) {
            return formatBigInteger((BigInteger) x);
        } else if (x instanceof Collection) {
            return toString((Collection<?>) x, dateTimeZone, dateTimeTimeZone);
        } else if (x instanceof Map) {
            return formatMap((Map<?, ?>) x, dateTimeZone, dateTimeTimeZone);
        } else if (x.getClass().isArray()) {
            return arrayToString(x, dateTimeZone, dateTimeTimeZone);
        } else {
            return String.valueOf(x);
        }
    }

    public static boolean needsQuoting(Object o) {
        return o != null
                && !(o instanceof Array)
                && !(o instanceof Boolean)
                && !(o instanceof Collection)
                // || o instanceof Iterable
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

    private static String toString(Collection<?> collection, TimeZone dateTimeZone,
                                  TimeZone dateTimeTimeZone)
    {
        return toString(collection.toArray(), dateTimeZone, dateTimeTimeZone);
    }

    public static String arrayToString(Object object, TimeZone dateTimeZone,
                                       TimeZone dateTimeTimeZone)
    {
        if (!object.getClass().isArray()) {
            throw new IllegalArgumentException("Object must be array");
        }
        if (object.getClass().getComponentType().isPrimitive()) {
            return primitiveArrayToString(object);
        }
        return toString((Object[]) object, dateTimeZone, dateTimeTimeZone);
    }

    private static String toString(Object[] values, TimeZone dateTimeZone, TimeZone dateTimeTimeZone) {
        if (values.length > 0 && values[0] != null && (values[0].getClass().isArray() || values[0] instanceof Collection)) {
            // quote is false to avoid escaping inner '['
            ArrayBuilder builder = new ArrayBuilder(false, dateTimeZone, dateTimeTimeZone);
            for (Object value : values) {
                if (value instanceof Collection) {
                    Object[] objects = ((Collection<?>) value).toArray();
                    builder.append(toString(objects, dateTimeZone, dateTimeTimeZone));
                } else {
                    builder.append(arrayToString(value, dateTimeZone, dateTimeTimeZone));
                }
            }
            return builder.build();
        }
        ArrayBuilder builder = new ArrayBuilder(needQuote(values), dateTimeZone, dateTimeTimeZone);
        for (Object value : values) {
            builder.append(value);
        }
        return builder.build();
    }

    private static String primitiveArrayToString(Object array) {
        if (array instanceof int[]) {
            return toString((int[]) array);
        } else if (array instanceof long[]) {
            return toString((long[]) array);
        } else if (array instanceof float[]) {
            return toString((float[]) array);
        } else if (array instanceof double[]) {
            return toString((double[]) array);
        } else if (array instanceof char[]) {
            return toString((char[]) array);
        } else if (array instanceof byte[]) {
            return toString((byte[]) array);
        } else if (array instanceof short[]) {
            return toString((short[]) array);
        } else {
            throw new IllegalArgumentException("Wrong primitive type: " + array.getClass().getComponentType());
        }
    }

    private static boolean needQuote(Object[] objects) {
        Object o = null;
        for (Object u : objects) {
            if (u != null) {
                o = u;
                break;
            }
        }
        return objects.length == 0 || needsQuoting(o);
    }

    private static String toString(int[] values) {
        ArrayBuilder builder = new ArrayBuilder(false);
        for (int value : values) {
            builder.append(value);
        }
        return builder.build();
    }

    private static String toString(long[] values) {
        ArrayBuilder builder = new ArrayBuilder(false);
        for (long value : values) {
            builder.append(value);
        }
        return builder.build();
    }

    private static String toString(float[] values) {
        ArrayBuilder builder = new ArrayBuilder(false);
        for (float value : values) {
            builder.append(value);
        }
        return builder.build();
    }

    private static String toString(double[] values) {
        ArrayBuilder builder = new ArrayBuilder(false);
        for (double value : values) {
            builder.append(value);
        }
        return builder.build();
    }

    private static String toString(byte[] values) {
        return "'" + formatBytes(values) + "'";
    }

    private static String toString(short[] values) {
        ArrayBuilder builder = new ArrayBuilder(false);
        for (short value : values) {
            builder.append(value);
        }
        return builder.build();
    }

    private static String toString(char[] values) {
        ArrayBuilder builder = new ArrayBuilder(true);
        for (char value : values) {
            builder.append(value);
        }
        return builder.build();
    }

    private static class ArrayBuilder {

        private final StringBuilder builder;
        private final boolean quote;
        private final TimeZone dateTimeZone;
        private final TimeZone dateTimeTimeZone;
        private int size = 0;
        private boolean built = false;

        private ArrayBuilder(boolean quote) {
            this(quote, TimeZone.getDefault(), TimeZone.getDefault());
        }

        private ArrayBuilder(boolean quote, TimeZone dateTimeZone,
                             TimeZone dateTimeTimeZone)
        {
            this.quote = quote;
            this.builder = new StringBuilder("[");
            this.dateTimeZone = dateTimeZone;
            this.dateTimeTimeZone = dateTimeTimeZone;
        }

        private ArrayBuilder append(Object value) {
            if (built) {
                throw new IllegalStateException("Already built");
            }
            if (size > 0) {
                builder.append(',');
            }
            if (value != null) {
                if (quote) {
                    builder.append('\'');
                }
                if (value instanceof String) {
                    builder.append(quote ? escape((String) value) : value);
                } else {
                    builder.append(formatObject(
                            value, dateTimeZone, dateTimeTimeZone));
                }
                if (quote) {
                    builder.append('\'');
                }
            } else {
                builder.append("NULL");
            }
            size++;
            return this;
        }

        private String build() {
            if (!built) {
                builder.append(']');
                built = true;
            }
            return builder.toString();
        }
    }
}
