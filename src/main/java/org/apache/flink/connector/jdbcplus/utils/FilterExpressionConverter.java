package org.apache.flink.connector.jdbcplus.utils;

import org.apache.flink.table.expressions.*;
import org.apache.flink.table.functions.FunctionDefinition;

import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;

import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.joining;


/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2022-06-25
 * @Description:
 */
public class FilterExpressionConverter implements ExpressionVisitor<Optional<String>> {

    List<ResolvedExpression> acceptedFilters;
    List<ResolvedExpression> remainingFilters;

    public FilterExpressionConverter() {
        this.acceptedFilters = new ArrayList<>();
        this.remainingFilters = new ArrayList<>();
    }

    public String convert(List<ResolvedExpression> unconvertedExpressions) {
        return unconvertedExpressions
                .stream()
                .filter(this::isSupport)
                .map(expression -> expression.accept(this))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(joining(" AND "));
    }

    public List<ResolvedExpression> getAcceptedFilters() {
        return acceptedFilters;
    }

    public List<ResolvedExpression> getRemainingFilters() {
        return remainingFilters;
    }

    boolean isSupport(ResolvedExpression resolvedExpression) {
        FunctionDefinition function = ((CallExpression) resolvedExpression).getFunctionDefinition();

        if (SupportFilter.contain(function)) {
            this.acceptedFilters.add(resolvedExpression);
            return true;
        } else {
            this.remainingFilters.add(resolvedExpression);
            return false;
        }
    }

    @Override
    public Optional<String> visit(CallExpression call) {
        FunctionDefinition function = call.getFunctionDefinition();
        SqlClause sqlClause = SupportFilter.getFilterClause(function);
        List<ResolvedExpression> childrenExpression = call.getResolvedChildren();

        List<String> args = childrenExpression
                .stream()
                .map(resolvedExpression -> resolvedExpression.accept(this))
                .map(Optional::get)
                .collect(Collectors.toList());

        String filterClause = sqlClause.formatter.apply(args);

        if (childrenExpression.size() > 1) {
            filterClause = String.join("", "(", filterClause, ")");
        }

        return Optional.ofNullable(filterClause);
    }

    @Override
    public Optional<String> visit(ValueLiteralExpression valueLiteral) {
        return convertValueLiteral(valueLiteral);
    }

    @Override
    public Optional<String> visit(FieldReferenceExpression fieldReference) {
        return convertFieldReference(fieldReference);
    }

    @Override
    public Optional<String> visit(TypeLiteralExpression typeLiteral) {
        return convertTypeLiteral(typeLiteral);
    }

    @Override
    public Optional<String> visit(Expression other) {
        return Optional.empty();
    }

    private Optional<String> convertValueLiteral(ValueLiteralExpression expression) {
        return expression
                .getValueAs(expression.getOutputDataType().getLogicalType().getDefaultConversion())
                .map(
                        o -> {
                            TimeZone timeZone = getTimeZone();
                            String value;
                            if (o instanceof Time) {
                                value =
                                        JdbcValueFormatter.formatTimestamp(
                                                toFixedDateTimestamp(((Time) o).toLocalTime()),
                                                timeZone);
                            } else if (o instanceof LocalTime) {
                                value =
                                        JdbcValueFormatter.formatTimestamp(
                                                toFixedDateTimestamp((LocalTime) o), timeZone);
                            } else if (o instanceof Instant) {
                                value =
                                        JdbcValueFormatter.formatTimestamp(
                                                Timestamp.from((Instant) o), timeZone);
                            } else {
                                value =
                                        JdbcValueFormatter.formatObject(
                                                o, timeZone, timeZone);
                            }

                            value =
                                    JdbcValueFormatter.needsQuoting(o)
                                            ? String.join("", "'", value, "'")
                                            : value;
                            return value;
                        });
    }

    private Optional<String> convertFieldReference(FieldReferenceExpression expression) {
        return Optional.of(quoteIdentifier(expression.getName()));
    }

    private Optional<String> convertTypeLiteral(TypeLiteralExpression expression) {
        return Optional.of(expression.getOutputDataType().getLogicalType().asSummaryString());
    }

    private String quoteIdentifier(String identifier) {
        return String.join("", "`", identifier, "`");
    }

    private TimeZone getTimeZone() {
        return TimeZone.getDefault();
    }

    private Timestamp toFixedDateTimestamp(LocalTime localTime) {
        LocalDateTime localDateTime = localTime.atDate(LocalDate.ofEpochDay(1));
        return Timestamp.valueOf(localDateTime);
    }
}
