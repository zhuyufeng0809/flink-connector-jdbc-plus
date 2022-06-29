package org.apache.flink.connector.jdbcplus.utils;

import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests for the {@link JdbcValueFormatter}.
 */
public class JdbcValueFormatterTest {

    private Map<String, Object> supportDataType;
    private Map<String, Object> unsupportedDataType;

    @BeforeAll
    void PrepareDataType() {
        this.supportDataType = new HashMap<>();
        this.unsupportedDataType = new HashMap<>();

        this.supportDataType.put("Int", 1337);
        this.supportDataType.put("Float", 4.2f);
        this.supportDataType.put("Double", 23.42);
        this.supportDataType.put("Long", -23L);
        this.supportDataType.put("BigDecimal", BigDecimal.valueOf(42.23));
        this.supportDataType.put("Short", Short.valueOf("-23"));
        this.supportDataType.put("NormalString", "foo");
        this.supportDataType.put("StringWithSymbol", "f'oo\to");
        this.supportDataType.put("Boolean", Boolean.TRUE);
        this.supportDataType.put("Date", new Date(1557136800000L));
        this.supportDataType.put("Time", new Time(1557136800000L));
        this.supportDataType.put("Timestamp", new Timestamp(1557136800000L));
        this.supportDataType.put("BigInteger", BigInteger.valueOf(1337));

        this.supportDataType.put("LocalDate", LocalDate.of(2020, 1, 7));
        this.supportDataType.put("LocalDateTime", LocalDateTime.of(2020, 1, 7, 13, 37, 42, 23));
        this.supportDataType.put("LocalTime", LocalTime.parse("13:37:42.023"));
        this.supportDataType.put("OffsetTime", BigInteger.valueOf(1337));
        this.supportDataType.put("BigInteger", BigInteger.valueOf(1337));
        this.supportDataType.put("BigInteger", BigInteger.valueOf(1337));
    }

    @Test
    void shouldReturnFormatValueWhenAcceptSupportDataType() {

    }

    @Test
    void shouldReturnNullWhenAcceptUnsupportedDataType() {

    }
}
