package com.hzcominfo.dataggr.uniquery.es5;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.sort.SortOrder;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.hzcominfo.dataggr.uniquery.AggItem;
import com.hzcominfo.dataggr.uniquery.FieldItem;
import com.hzcominfo.dataggr.uniquery.GroupItem;
import com.hzcominfo.dataggr.uniquery.JsonBasicVisitor;
import com.hzcominfo.dataggr.uniquery.OrderItem;
import com.hzcominfo.dataggr.uniquery.TableItem;

public class SearchRequestBuilderVistor extends JsonBasicVisitor<SearchRequestBuilder> {

    public static final String AGGRS_SUFFIX = "_aggrs";
    private List<AggItem> aggItems = new ArrayList<>();
    private List<FieldItem> fieldItems = new ArrayList<>();

    public SearchRequestBuilderVistor(SearchRequestBuilder searchRequestBuilder, JsonObject json) {
        super(searchRequestBuilder, json);
        visit(json);
    }

    @Override
    public void visitFields(List<FieldItem> fields, boolean distinct) {
        SearchRequestBuilder builder = super.get();
        this.fieldItems = new ArrayList<>(fields);
        boolean isStart = fields.stream().map(FieldItem::name).collect(Collectors.toSet()).contains("*");
        if (!isStart) {
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

    @Override
    public void visitGroupBy(List<GroupItem> groups) {
        if (null == groups || groups.isEmpty()) return;
        AggregationBuilder subBuilder = null;
        List<AggregationBuilder> aggs = aggItems.stream().map(SearchRequestBuilderVistor::toAggregationBuilder).collect(Collectors.toList());
        for (int i = groups.size() - 1; i >= 0; i--) {
            GroupItem groupItem = groups.get(i);
            if (i == groups.size() - 1) {
                subBuilder = AggregationBuilders.terms(groupItem.name() + AGGRS_SUFFIX).field(groupItem.name());
                for (AggregationBuilder agg : aggs) subBuilder.subAggregation(agg);
            } else {
                subBuilder = AggregationBuilders.terms(groupItem.name() + AGGRS_SUFFIX).field(groupItem.name()).subAggregation(subBuilder);
            }
        }
        System.out.println(subBuilder);
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
            builder.setSize((int) limit);
        }
    }

	@Override
	public void visitOnlyCount(boolean onlyCount) {
		// TODO Auto-generated method stub
		JsonArray es5Fields = new JsonArray();
		if (fieldItems.isEmpty()) return;
        for (FieldItem item : fieldItems) {
            if ("*".equals(item.name())) return;
            es5Fields.add(item.name());
        }
        
        List<String> funcFields = new ArrayList<>();
		for(JsonElement fieldElement : es5Fields) {
			String field = fieldElement.getAsString();
			if (field.contains("(") && field.contains(")")) {
				funcFields.add(field);
			}
		}
		if (!funcFields.isEmpty() && funcFields.size() == 1 && funcFields.get(0).startsWith("count"))
			onlyCount = true;
		if (onlyCount) {
			SearchRequestBuilder builder = super.get();
			builder.setSize(0);
		}
	}
}
