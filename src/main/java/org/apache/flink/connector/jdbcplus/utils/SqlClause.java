package org.apache.flink.connector.jdbcplus.utils;

import java.util.function.Function;

/** SQL filters that support push down. */
public enum SqlClause {
    EQ(args -> String.format("%s = %s", args[0], args[1])),

    NOT_EQ(args -> String.format("%s <> %s", args[0], args[1])),

    GT(args -> String.format("%s > %s", args[0], args[1])),

    GT_EQ(args -> String.format("%s >= %s", args[0], args[1])),

    LT(args -> String.format("%s < %s", args[0], args[1])),

    LT_EQ(args -> String.format("%s <= %s", args[0], args[1])),

    IS_NULL(args -> String.format("%s IS NULL", args[0])),

    IS_NOT_NULL(args -> String.format("%s IS NOT NULL", args[0])),

    AND(args -> String.format("%s AND %s", args[0], args[1])),

    OR(args -> String.format("%s OR %s", args[0], args[1])),

    CAST(args -> String.format("CAST(%s AS %s)", args[0], args[1])),

    LIKE(args -> String.format("%s LIKE %s", args[0], args[1])),

    NOT(args -> String.format("!(%s)", args[0]));

    public final Function<String[], String> formatter;

    SqlClause(final Function<String[], String> function) {
        this.formatter = function;
    }
}
