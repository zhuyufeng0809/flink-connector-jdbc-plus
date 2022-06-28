package org.apache.flink.connector.jdbcplus.utils;

import java.util.List;
import java.util.function.Function;

/** SQL filters that support push down. */
public enum SqlClause {
    EQ(list -> String.format("%s = %s", list.get(0), list.get(1)), 2),

    NOT_EQ(list -> String.format("%s <> %s", list.get(0), list.get(1)), 2),

    GT(list -> String.format("%s > %s", list.get(0), list.get(1)), 2),

    GT_EQ(list -> String.format("%s >= %s", list.get(0), list.get(1)), 2),

    LT(list -> String.format("%s < %s", list.get(0), list.get(1)), 2),

    LT_EQ(list -> String.format("%s <= %s", list.get(0), list.get(1)), 2),

    IS_NULL(list -> String.format("%s IS NULL", list.get(0)), 1),

    IS_NOT_NULL(list -> String.format("%s IS NOT NULL", list.get(0)), 1),

    AND(list -> String.format("%s AND %s", list.get(0), list.get(1)), 2),

    OR(list -> String.format("%s OR %s", list.get(0), list.get(1)), 2),

//    CAST(list -> String.format("CAST(%s AS %s)", list.get(0), list.get(1))),

    LIKE(list -> String.format("%s LIKE %s", list.get(0), list.get(1)), 2),

    NOT(list -> String.format("!(%s)", list.get(0)), 1);

    public final Function<List<String>, String> formatter;
    public final int argsNum;

    SqlClause(final Function<List<String>, String> function, int argsNum) {
        this.formatter = function;
        this.argsNum = argsNum;
    }
}
