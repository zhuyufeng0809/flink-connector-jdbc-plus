package org.apache.flink.connector.jdbcplus.utils;

import org.apache.flink.connector.jdbcplus.dialect.JdbcDialect;
import org.apache.flink.table.expressions.*;
import org.apache.flink.table.functions.FunctionDefinition;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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
    JdbcDialect dialect;

    public FilterExpressionConverter(JdbcDialect dialect) {
        this.dialect = dialect;
        this.acceptedFilters = new ArrayList<>();
        this.remainingFilters = new ArrayList<>();
    }

    public String convert(List<ResolvedExpression> unconvertedExpressions) {
        return unconvertedExpressions
                .stream()
                .map(resolvedExpression -> {
                    Optional<String> convertedFilter = resolvedExpression.accept(this);

                    if (convertedFilter.isPresent()) {
                        this.acceptedFilters.add(resolvedExpression);
                    } else {
                        this.remainingFilters.add(resolvedExpression);
                    }

                    return convertedFilter;
                })
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

    @Override
    public Optional<String> visit(CallExpression call) {
        FunctionDefinition function = call.getFunctionDefinition();

        FilterClause filterClause = dialect.getFilterClause(function);
        if (filterClause == null) {
            return Optional.empty();
        }

        List<String> args = call
                .getResolvedChildren()
                .stream()
                .map(resolvedExpression -> resolvedExpression.accept(this))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());

        if (args.size() == filterClause.getArgsNum()) {
            return Optional.of(String.join("", "(", filterClause.apply(args), ")"));
        } else {
            return Optional.empty();
        }
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
                .map(JdbcValueFormatter::formatObject);
    }

    private Optional<String> convertFieldReference(FieldReferenceExpression expression) {
        return Optional.of(quoteIdentifier(expression.getName()));
    }

    private Optional<String> convertTypeLiteral(TypeLiteralExpression expression) {
        return Optional.of(expression.getOutputDataType().getLogicalType().asSummaryString().replaceAll(" NOT NULL", ""));
    }

    private String quoteIdentifier(String identifier) {
        return String.join("", "`", identifier, "`");
    }
}
