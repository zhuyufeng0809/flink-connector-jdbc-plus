package org.apache.flink.connector.jdbcplus.utils;

import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.connector.jdbcplus.utils.SqlClause.*;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2022-06-25
 * @Description:
 */
public class SupportFilter {
    private static final Map<FunctionDefinition, SqlClause> FILTERS = new HashMap<>();

    static {
        FILTERS.put(BuiltInFunctionDefinitions.EQUALS, EQ);
        FILTERS.put(BuiltInFunctionDefinitions.NOT_EQUALS, NOT_EQ);
        FILTERS.put(BuiltInFunctionDefinitions.GREATER_THAN, GT);
        FILTERS.put(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL, GT_EQ);
        FILTERS.put(BuiltInFunctionDefinitions.LESS_THAN, LT);
        FILTERS.put(BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL, LT_EQ);
        FILTERS.put(BuiltInFunctionDefinitions.IS_NULL, IS_NULL);
        FILTERS.put(BuiltInFunctionDefinitions.IS_NOT_NULL, IS_NOT_NULL);
        FILTERS.put(BuiltInFunctionDefinitions.AND, AND);
        FILTERS.put(BuiltInFunctionDefinitions.OR, OR);
//        FILTERS.put(BuiltInFunctionDefinitions.CAST, CAST);
        FILTERS.put(BuiltInFunctionDefinitions.LIKE, LIKE);
        FILTERS.put(BuiltInFunctionDefinitions.NOT, NOT);
    }

    private SupportFilter() {}

    public static boolean contain(FunctionDefinition function) {
        return FILTERS.containsKey(function);
    }

    public static SqlClause getFilterClause(FunctionDefinition function) {
        return FILTERS.get(function);
    }
}
