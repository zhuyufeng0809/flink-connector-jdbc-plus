package org.apache.flink.connector.jdbcplus.utils;

import org.apache.flink.table.expressions.*;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import ru.yandex.clickhouse.util.ClickHouseValueFormatter;

import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.function.Function;

import static java.util.stream.Collectors.joining;
import static org.apache.flink.connector.jdbcplus.utils.SqlClause.*;

/** Filter push down, convert flink expression to mysql filter clause. */
public class FilterPushDownHelper {

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
        FILTERS.put(BuiltInFunctionDefinitions.CAST, CAST);
        FILTERS.put(BuiltInFunctionDefinitions.LIKE, LIKE);
        FILTERS.put(BuiltInFunctionDefinitions.NOT, NOT);
    }

    private FilterPushDownHelper() {}

    public static String convert(List<ResolvedExpression> filters) {
        int filterSize = filters.size();
        return filters.stream()
                .map(expression -> FilterPushDownHelper.convertExpression(expression, filterSize))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(joining(" AND "));
    }

    private static Optional<String> convertExpression(
            ResolvedExpression resolvedExpression, int filterSize) {
        if ((resolvedExpression instanceof ValueLiteralExpression)) {
            return convertValueLiteral((ValueLiteralExpression) resolvedExpression);
        }

        if ((resolvedExpression instanceof FieldReferenceExpression)) {
            return convertField((FieldReferenceExpression) resolvedExpression);
        }

        if (!(resolvedExpression instanceof CallExpression)) {
            return Optional.empty();
        }

        CallExpression call = (CallExpression) resolvedExpression;
        SqlClause sqlClause = FILTERS.get(call.getFunctionDefinition());
        if (sqlClause == null) {
            return Optional.empty();
        }

        switch (sqlClause) {
            case EQ:
                return convertLogicExpression(EQ.formatter, call, filterSize);
            case NOT_EQ:
                return convertLogicExpression(NOT_EQ.formatter, call, filterSize);
            case GT:
                return convertLogicExpression(GT.formatter, call, filterSize);
            case GT_EQ:
                return convertLogicExpression(GT_EQ.formatter, call, filterSize);
            case LT:
                return convertLogicExpression(LT.formatter, call, filterSize);
            case LT_EQ:
                return convertLogicExpression(LT_EQ.formatter, call, filterSize);
            case OR:
                return convertLogicExpression(OR.formatter, call, filterSize);
            case AND:
                return convertLogicExpression(AND.formatter, call, filterSize);
            case CAST:
                return convertFieldAndLiteral(CAST.formatter, call);
            case LIKE:
                return convertFieldAndLiteral(LIKE.formatter, call);
            case IS_NULL:
                return convertOnlyChild(IS_NULL.formatter, call);
            case IS_NOT_NULL:
                return convertOnlyChild(IS_NOT_NULL.formatter, call);
            case NOT:
                return convertOnlyChild(NOT.formatter, call);
            default:
                return Optional.empty();
        }
    }

    private static Optional<String> convertOnlyChild(
            Function<String[], String> sqlClauseFormatter, CallExpression call) {
        List<ResolvedExpression> children = call.getResolvedChildren();
        if (children.size() != 1) {
            return Optional.empty();
        }

        ResolvedExpression child = children.get(0);

        if (child instanceof CallExpression) {
            Optional<String> sqlClause = convertExpression(child, 1);
            return sqlClause.map(s -> sqlClauseFormatter.apply(new String[]{s}));
        }

        if (!(child instanceof FieldReferenceExpression)) {
            return Optional.empty();
        }

        FieldReferenceExpression fieldExpression = (FieldReferenceExpression) child;
        String fieldName = quoteIdentifier(fieldExpression.getName());
        return Optional.of(sqlClauseFormatter.apply(new String[] {fieldName}));
    }

    private static Optional<String> convertLogicExpression(
            Function<String[], String> sqlClauseFormatter, CallExpression call, int filterSize) {
        List<ResolvedExpression> args = call.getResolvedChildren();
        if (args.size() != 2) {
            return Optional.empty();
        }

        String left = convertExpression(args.get(0), args.size()).orElse(null);
        String right = convertExpression(args.get(1), args.size()).orElse(null);
        if (left == null || right == null) {
            return Optional.empty();
        }

        String sqlClause = sqlClauseFormatter.apply(new String[] {left, right});
        if (filterSize > 1) {
            sqlClause = String.join("", "(", sqlClause, ")");
        }
        return Optional.of(sqlClause);
    }

    private static Optional<String> convertFieldAndLiteral(
            Function<String[], String> sqlClauseFormatter, CallExpression callExpression) {
        List<ResolvedExpression> args = callExpression.getResolvedChildren();
        if (args.size() != 2) {
            return Optional.empty();
        }

        FieldReferenceExpression fieldExpression =
                args.stream()
                        .filter(expression -> expression instanceof FieldReferenceExpression)
                        .map(expression -> ((FieldReferenceExpression) expression))
                        .findAny()
                        .orElse(null);
        if (fieldExpression == null) {
            return Optional.empty();
        }
        String fieldName = quoteIdentifier(fieldExpression.getName());

        ValueLiteralExpression literalExpression =
                args.stream()
                        .filter(expression -> expression instanceof ValueLiteralExpression)
                        .map(expression -> (ValueLiteralExpression) expression)
                        .findAny()
                        .orElse(null);
        if (literalExpression != null) {
            String literalValue = convertValueLiteral(literalExpression).orElse(null);
            return Optional.of(sqlClauseFormatter.apply(new String[] {fieldName, literalValue}));
        }

        TypeLiteralExpression typeLiteralExpression =
                args.stream()
                        .filter(expression -> expression instanceof TypeLiteralExpression)
                        .map(expression -> (TypeLiteralExpression) expression)
                        .findAny()
                        .orElse(null);
        if (typeLiteralExpression != null) {
            String typeLiteral = convertTypeLiteral(typeLiteralExpression).orElse(null);
            return Optional.of(sqlClauseFormatter.apply(new String[] {fieldName, typeLiteral}));
        }

        return Optional.empty();
    }

    private static Optional<String> convertValueLiteral(ValueLiteralExpression expression) {
        return expression
                .getValueAs(expression.getOutputDataType().getLogicalType().getDefaultConversion())
                .map(
                        o -> {
                            TimeZone timeZone = getFlinkTimeZone();
                            String value;
                            if (o instanceof Time) {
                                value =
                                        ClickHouseValueFormatter.formatTimestamp(
                                                toFixedDateTimestamp(((Time) o).toLocalTime()),
                                                timeZone);
                            } else if (o instanceof LocalTime) {
                                value =
                                        ClickHouseValueFormatter.formatTimestamp(
                                                toFixedDateTimestamp((LocalTime) o), timeZone);
                            } else if (o instanceof Instant) {
                                value =
                                        ClickHouseValueFormatter.formatTimestamp(
                                                Timestamp.from((Instant) o), timeZone);
                            } else {
                                value =
                                        ClickHouseValueFormatter.formatObject(
                                                o, timeZone, timeZone);
                            }

                            value =
                                    ClickHouseValueFormatter.needsQuoting(o)
                                            ? String.join("", "'", value, "'")
                                            : value;
                            return value;
                        });
    }

    private static Optional<String> convertField(FieldReferenceExpression expression) {
        return Optional.of(quoteIdentifier(expression.getName()));
    }

    private static Optional<String> convertTypeLiteral(TypeLiteralExpression expression) {
        return Optional.of(expression.getOutputDataType().getLogicalType().asSummaryString().replaceAll(" .*", ""));
    }

    private static String quoteIdentifier(String identifier) {
        return String.join("", "`", identifier, "`");
    }

    public static Timestamp toFixedDateTimestamp(LocalTime localTime) {
        LocalDateTime localDateTime = localTime.atDate(LocalDate.ofEpochDay(1));
        return Timestamp.valueOf(localDateTime);
    }

    private static TimeZone getFlinkTimeZone() {
        return TimeZone.getDefault();
    }
}
