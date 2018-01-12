package com.hzcominfo.dataggr.uniquery.es5;

import com.google.gson.JsonObject;
import com.hzcominfo.dataggr.uniquery.*;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.sort.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.hzcominfo.dataggr.uniquery.es5.Es5ConditionTransverter.isNestedField;
import static com.hzcominfo.dataggr.uniquery.es5.Es5ConditionTransverter.nestedFieldPath;

public class SearchRequestBuilderVisitor extends JsonBasicVisitor<SearchRequestBuilder> {

    public static final String AGGRS_SUFFIX = "_aggrs";
    private List<AggItem> aggItems = new ArrayList<>();

    public SearchRequestBuilderVisitor(SearchRequestBuilder searchRequestBuilder, JsonObject json) {
        super(searchRequestBuilder, json);
        visit(json);
    }

    @Override
    public void visitFields(List<FieldItem> fields, boolean distinct) {
        SearchRequestBuilder builder = super.get();
        boolean isStart = fields.stream().map(FieldItem::name).collect(Collectors.toSet()).contains("*");
        if (!isStart) {
            builder.setFetchSource(fields.stream()
                    .map(FieldItem::full)
                    .filter(field -> !field.contains("(") && !field.contains(")"))
                    .toArray(String[]::new), new String[]{});
        }
        fields.stream()
                .filter(fieldItem -> fieldItem.full().contains("(") && fieldItem.full().contains(")"))
                .map(fieldItem -> AggItem.of(fieldItem.full(), fieldItem.alias()))
                .forEach(item -> aggItems.add(item));
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

    @Override
    public void visitGroupBy(List<GroupItem> groups) {
        if (null == groups || groups.isEmpty()) return;
        AggregationBuilder subBuilder = null;
        List<AggregationBuilder> aggs = aggItems.stream().map(SearchRequestBuilderVisitor::toAggregationBuilder).collect(Collectors.toList());
        for (int i = groups.size() - 1; i >= 0; i--) {
            GroupItem groupItem = groups.get(i);
            if (i == groups.size() - 1) {
                subBuilder = AggregationBuilders.terms(groupItem.name() + AGGRS_SUFFIX).field(groupItem.name());
                for (AggregationBuilder agg : aggs) subBuilder.subAggregation(agg);
            } else {
                subBuilder = AggregationBuilders.terms(groupItem.name() + AGGRS_SUFFIX).field(groupItem.name()).subAggregation(subBuilder);
            }
        }
//        System.out.println("subBuilder:" + subBuilder);
        SearchRequestBuilder builder = super.get();
        builder.setSize(0);
        builder.addAggregation(subBuilder);
    }

    private AggregationBuilder toSubTermAggregationBuilder(List<GroupItem> groups) {
        if (null == groups || groups.isEmpty()) return null;
        GroupItem first = groups.remove(0);
        AggregationBuilder parent = AggregationBuilders.terms(first.name()).field(first.name());
        AggregationBuilder child = toSubTermAggregationBuilder(groups);
        if (null != child) parent.subAggregation(child);
        return parent;
    }

    @Override
    public void visitMultiGroupBy(List<List<GroupItem>> groupsList) {
        if (null == groupsList || groupsList.isEmpty()) return;
        SearchRequestBuilder builder = super.get();

        groupsList.stream().filter(list -> list != null && !list.isEmpty()).forEach(list ->
                builder.addAggregation(toSubTermAggregationBuilder(list)));
    }

    @Override
    public void visitOrderBy(List<OrderItem> orders) {
        assert null != orders;
        SearchRequestBuilder builder = super.get();
        orders.forEach(order -> {
            String field = order.name();
            SortOrder sortOrder = order.desc() ? SortOrder.DESC : SortOrder.ASC;
            if (field.equals(ScoreSortBuilder.NAME)) {
                ScoreSortBuilder scoreSortBuilder = new ScoreSortBuilder();
                // https://www.elastic.co/guide/en/elasticsearch/reference/5.2/search-request-sort.html#_sort_order
                scoreSortBuilder.order(sortOrder); // 默认是降序的
                builder.addSort(scoreSortBuilder);
            } else {
                FieldSortBuilder fieldSortBuilder = nestedFieldSortBuilderWrapper(
                        // https://www.elastic.co/guide/en/elasticsearch/reference/5.2/search-request-sort.html#_sort_order
                        SortBuilders.fieldSort(field).order(sortOrder)); // 默认是升序的
                builder.addSort(fieldSortBuilder);
            } // no script sort support
        });
    }

    private FieldSortBuilder nestedFieldSortBuilderWrapper(FieldSortBuilder builder) {
        String field = builder.getFieldName();
        if (isNestedField(field)) {
            throw new RuntimeException("`ORDER BY` NOT support nested field for now!");
            // min max can use anywhere ? sum, avg, median cant use only in number based array field
//            builder.sortMode(); // default sort mode is null;
//            builder.setNestedPath(nestedFieldPath(field)); // open this line to support nested field sort
        }
        return builder;
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
            builder.setSize((int) limit);
        }
    }

	@Override
	public void visitIsCount(boolean isGroup) {
		if (isGroup) return;
		if (aggItems.size() == 1 && aggItems.get(0).function().equals("COUNT")) {
			SearchRequestBuilder builder = super.get();
			builder.setSize(0);
		}
	}
}
