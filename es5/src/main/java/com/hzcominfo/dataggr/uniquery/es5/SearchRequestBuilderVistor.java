package com.hzcominfo.dataggr.uniquery.es5;

import com.google.gson.JsonObject;
import com.hzcominfo.dataggr.uniquery.*;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.hzcominfo.dataggr.uniquery.es5.Es5ConditionTransverter.KEYWORD_SUFFIX;

public class SearchRequestBuilderVistor extends JsonBasicVisitor<SearchRequestBuilder> {

    public SearchRequestBuilderVistor(SearchRequestBuilder searchRequestBuilder, JsonObject json) {
        super(searchRequestBuilder, json);
        visit(json);
    }

    @Override
    public void visitFields(List<FieldItem> fields, boolean distinct) {
        SearchRequestBuilder builder = super.get();
        boolean isStart = fields.stream().map(FieldItem::name).collect(Collectors.toSet()).contains("*");
        if (! isStart) {
            builder.setFetchSource(fields.stream().map(FieldItem::name).toArray(String[]::new), new String[]{});
        }
    }

    @Override
    public void visitTables(List<TableItem> tables) {
        assert null != tables;
        SearchRequestBuilder builder = super.get();

        String[] types = tables.stream().map(table -> table.name(0)).toArray(String[]::new);
        String[] indices = tables.stream().map(table -> table.name(1)).filter(Objects::nonNull).toArray(String[]::new);
        builder.setIndices(indices);
        builder.setTypes(types);
    }

    @Override
    public void visitConditions(JsonObject json) {
        SearchRequestBuilder builder = super.get();
        QueryBuilder query = Es5ConditionTransverter.of(json);
        System.out.println("==============query===============================");
        System.out.println(query);
        builder.setQuery(query);
    }

    @Override
    public void visitGroupBy(List<GroupItem> groups) {

    }

    @Override
    public void visitMultiGroupBy(List<List<GroupItem>> groupsList) {

    }

    @Override
    public void visitOrderBy(List<OrderItem> orders) {
        assert null != orders;
        SearchRequestBuilder builder = super.get();
        orders.forEach(order -> builder.addSort(order.name() + KEYWORD_SUFFIX, order.desc() ? SortOrder.DESC : SortOrder.ASC));
    }

    @Override
    public void visitHaving() {

    }

    @Override
    public void visitOffset(long offset) {
        if (offset > 0L) {
            SearchRequestBuilder builder = super.get();
            builder.setFrom((int) offset);
        }
    }

    @Override
    public void visitLimit(long limit) {
        if (limit >= 0L) {
            SearchRequestBuilder builder = super.get();
            builder.setSize((int)limit);
        }
    }
}