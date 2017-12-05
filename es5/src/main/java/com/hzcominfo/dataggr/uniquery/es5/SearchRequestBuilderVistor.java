package com.hzcominfo.dataggr.uniquery.es5;

import com.google.gson.JsonObject;
import com.hzcominfo.dataggr.uniquery.*;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class SearchRequestBuilderVistor extends JsonBasicVisitor<SearchRequestBuilder> {

    private List<AggItem> aggItems = new ArrayList<>();

    public SearchRequestBuilderVistor(SearchRequestBuilder searchRequestBuilder, JsonObject json) {
        super(searchRequestBuilder, json);
        visit(json);
    }

    @Override
    public void visitFields(List<FieldItem> fields, boolean distinct) {
        SearchRequestBuilder builder = super.get();
        boolean isStart = fields.stream().map(FieldItem::name).collect(Collectors.toSet()).contains("*");
        if (! isStart) {
            builder.setFetchSource(fields.stream().map(FieldItem::name)
                    .filter(field -> !field.contains("(") && !field.contains(")")).toArray(String[]::new), new String[]{});
        }
        fields.stream().filter(fieldItem -> fieldItem.full().contains("(") && fieldItem.full().contains(")"))
                .map(fieldItem -> AggItem.of(fieldItem.full(), fieldItem.alias())).forEach(item -> aggItems.add(item));
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
        builder.setQuery(query);
    }

    private static AggregationBuilder toAggregationBuilder(AggItem item) {
        switch (item.function()) {
            case "COUNT":
                return AggregationBuilders.count(item.name()).field(item.field());
            case "SUM":
                return AggregationBuilders.sum(item.name()).field(item.field());
            case "AVG":
                return AggregationBuilders.avg(item.name()).field(item.field());
            case "MIN":
                return AggregationBuilders.min(item.name()).field(item.field());
            case "MAX":
                return AggregationBuilders.max(item.name()).field(item.field());
            default:
                throw new RuntimeException("Unsupported aggregation: " + item);
        }
    }
/*
    @Override
    public void visitGroupBy(List<GroupItem> groups) {
        if (null == groups || groups.isEmpty()) return;
        String first = groups.remove(0).name();
        AggregationBuilder aggBuilder = AggregationBuilders.terms("elastic_agg").field(first);
        AggregationBuilder subBuilder = null;
        if (!groups.isEmpty()) for (int i = groups.size() - 1; i >= 0; i--) {
            if (i == groups.size() - 1) {
                subBuilder = AggregationBuilders.terms(groups.get(i).name()).field(groups.get(i).name());
                aggItems.forEach(item -> {
                    subBuilder.subAggregation(toAggregationBuilder(item));
                });
            } else {
                subBuilder = AggregationBuilders.terms(groups.get(i).name()).field(groups.get(i).name())
                        .subAggregation(subBuilder);
            }
        } else {

        }

        groups.stream().map(GroupItem::name).forEach(field -> {
            AggregationBuilder sub = AggregationBuilders.terms("elastic_agg").field(field);

            sub.subAggregation(AggregationBuilders.count("count_sell").field("sell"));
            sub.subAggregation(AggregationBuilders.avg("avg_sell").field("sell"));
            aggBuilder.subAggregation(sub);
        });
        SearchRequestBuilder builder = super.get();
        builder.addAggregation(aggBuilder);
    }
    */
@Override
public void visitGroupBy(List<GroupItem> groups) {
    if (null == groups || groups.isEmpty()) return;
    AggregationBuilder subBuilder = null;
    List<AggregationBuilder> aggs = aggItems.stream().map(SearchRequestBuilderVistor::toAggregationBuilder).collect(Collectors.toList());
    for (int i = groups.size() - 1; i >= 0; i--) {
        GroupItem groupItem = groups.get(i);
        if (i == groups.size() - 1) {
            subBuilder = AggregationBuilders.terms(groupItem.name()).field(groupItem.name());
            for (AggregationBuilder agg : aggs) subBuilder.subAggregation(agg);
        } else {
            subBuilder = AggregationBuilders.terms(groupItem.name()).field(groupItem.name()).subAggregation(subBuilder);
        }
    }
    System.out.println(subBuilder);
    SearchRequestBuilder builder = super.get();
    builder.addAggregation(subBuilder);
}

    @Override
    public void visitMultiGroupBy(List<List<GroupItem>> groupsList) {

    }

    @Override
    public void visitOrderBy(List<OrderItem> orders) {
        assert null != orders;
        SearchRequestBuilder builder = super.get();
        orders.forEach(order -> builder.addSort(order.name(), order.desc() ? SortOrder.DESC : SortOrder.ASC));
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
