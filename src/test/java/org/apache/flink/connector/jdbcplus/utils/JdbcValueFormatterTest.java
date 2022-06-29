package org.apache.flink.connector.jdbcplus.utils;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.*;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the {@link JdbcValueFormatter}.
 */
public class JdbcValueFormatterTest {

    private static Map<String, Object> supportDataType;
    private static Map<String, Object> unsupportedDataType;

    @BeforeAll
    static void PrepareDataType() {
        supportDataType = new HashMap<>();
        unsupportedDataType = new HashMap<>();

        unsupportedDataType.put("Instant", Instant.now());

        supportDataType.put("Int", 1337);
        supportDataType.put("Float", 4.2f);
        supportDataType.put("Double", 23.42);
        supportDataType.put("Long", -23L);
        supportDataType.put("BigDecimal", BigDecimal.valueOf(42.23));
        supportDataType.put("Short", Short.valueOf("-23"));
        supportDataType.put("NormalString", "foo");
        supportDataType.put("StringWithSymbol", "f'oo\to");
        supportDataType.put("Boolean", Boolean.TRUE);
        supportDataType.put("Date", new Date(1557136800000L));
        supportDataType.put("Time", new Time(1557136800000L));
        supportDataType.put("Timestamp", new Timestamp(1557136800000L));
        supportDataType.put("BigInteger", BigInteger.valueOf(1337));
        supportDataType.put("LocalDate", LocalDate.of(2020, 1, 7));
        supportDataType.put("LocalDateTime", LocalDateTime.of(2020, 1, 7, 13, 37, 42, 23));
        supportDataType.put("LocalTime", LocalTime.parse("13:37:42.023"));
        supportDataType.put("OffsetTime",
                OffsetTime.of(LocalTime.parse("13:37:42.023"),
                        ZoneOffset.ofHoursMinutes(1, 7)));
        supportDataType.put("OffsetDateTime",
                OffsetDateTime.of(LocalDateTime.of(2020, 1, 7, 13, 37, 42, 107),
                        ZoneOffset.ofHoursMinutes(2, 30)));
    }

    @Test
    void testAcceptInt() {
        assertThat(JdbcValueFormatter.formatObject(supportDataType.get("Int")))
                .as("Test Int").isEqualTo("1337");
    }
    @Test
    void testAcceptFloat() {
        assertThat(JdbcValueFormatter.formatObject(supportDataType.get("Float")))
                .as("Test Float").isEqualTo("4.2");
    }
    @Test
    void testAcceptDouble() {
        assertThat(JdbcValueFormatter.formatObject(supportDataType.get("Double")))
                .as("Test Double").isEqualTo("23.42");
    }
    @Test
    void testAcceptLong() {
        assertThat(JdbcValueFormatter.formatObject(supportDataType.get("Long")))
                .as("Test Long").isEqualTo("-23");
    }
    @Test
    void testAcceptBigDecimal() {
        assertThat(JdbcValueFormatter.formatObject(supportDataType.get("BigDecimal")))
                .as("Test BigDecimal").isEqualTo("42.23");
    }
    @Test
    void testAcceptShort() {
        assertThat(JdbcValueFormatter.formatObject(supportDataType.get("Short")))
                .as("Test Short").isEqualTo("-23");
    }
    @Test
    void testAcceptNormalString() {
        assertThat(JdbcValueFormatter.formatObject(supportDataType.get("NormalString")))
                .as("Test NormalString").isEqualTo("'foo'");
    }
    @Test
    void testAcceptStringWithSymbol() {
        assertThat(JdbcValueFormatter.formatObject(supportDataType.get("StringWithSymbol")))
                .as("Test StringWithSymbol").isEqualTo("'f\\'oo\\to'");
    }
    @Test
    void testAcceptBoolean() {
        assertThat(JdbcValueFormatter.formatObject(supportDataType.get("Boolean")))
                .as("Test Boolean").isEqualTo("1");
    }
    @Test
    void testAcceptDate() {
        assertThat(JdbcValueFormatter.formatObject(supportDataType.get("Date")))
                .as("Test Date").isEqualTo("'2019-05-06'");
    }
    @Test
    void testAcceptTime() {
        assertThat(JdbcValueFormatter.formatObject(supportDataType.get("Time")))
                .as("Test Time").isEqualTo("'18:00:00'");
    }
    @Test
    void testAcceptTimestamp() {
        assertThat(JdbcValueFormatter.formatObject(supportDataType.get("Timestamp")))
                .as("Test Timestamp").isEqualTo("'2019-05-06 18:00:00'");
    }
    @Test
    void testAcceptBigInteger() {
        assertThat(JdbcValueFormatter.formatObject(supportDataType.get("BigInteger")))
                .as("Test BigInteger").isEqualTo("1337");
    }
    @Test
    void testAcceptLocalDate() {
        assertThat(JdbcValueFormatter.formatObject(supportDataType.get("LocalDate")))
                .as("Test LocalDate").isEqualTo("'2020-01-07'");
    }
    @Test
    void testAcceptLocalDateTime() {
        assertThat(JdbcValueFormatter.formatObject(supportDataType.get("LocalDateTime")))
                .as("Test LocalDateTime").isEqualTo("'2020-01-07 13:37:42'");
    }
    @Test
    void testAcceptLocalTime() {
        assertThat(JdbcValueFormatter.formatObject(supportDataType.get("LocalTime")))
                .as("Test LocalTime").isEqualTo("'13:37:42'");
    }
    @Test
    void testAcceptOffsetTime() {
        assertThat(JdbcValueFormatter.formatObject(supportDataType.get("OffsetTime")))
                .as("Test OffsetTime").isEqualTo("'13:37:42.023+01:07'");
    }
    @Test
    void testAcceptOffsetDateTime() {
        assertThat(JdbcValueFormatter.formatObject(supportDataType.get("OffsetDateTime")))
                .as("Test OffsetDateTime").isEqualTo("'2020-01-07 19:07:42'");
    }

    @Test
    void testAcceptUnsupportedDataType() {
        assertThat(JdbcValueFormatter.formatObject(unsupportedDataType.get("Instant")))
                .as("Test Unsupported Instant").isNull();
    }
}
