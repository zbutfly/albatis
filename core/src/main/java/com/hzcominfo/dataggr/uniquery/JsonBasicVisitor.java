package com.hzcominfo.dataggr.uniquery;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.List;

public abstract class JsonBasicVisitor<V> implements JsonVisitor<V> {
    private V v;

    public JsonBasicVisitor(V v, JsonObject json) {
        assert json != null;
        this.v = v;
//        visit(json);
    }

    @Override
    public V get() {
        return v;
    }

    protected void visit(JsonObject json) {
        JsonElement element;

        element = json.get("tables");
        List<TableItem> tables = new ArrayList<>();
        tables.add(TableItem.of(element.getAsJsonObject()));
        visitTables(tables);

        element = json.get("distinct");
        boolean distinct = null != element && element.getAsBoolean();
        List<FieldItem> fields = fieldsJsonArray2List(json.getAsJsonArray("fields"));
        visitFields(fields, distinct);

        visitConditions(json.get("where").getAsJsonObject());
        
        List<OrderItem> orders = ordersJsonArray2List(json.getAsJsonArray("orderBy"));
        visitOrderBy(orders);

        element = json.get("offset");
        if (null != element) {
            long offset = element.getAsLong();
            visitOffset(offset);
        }

        element = json.get("limit");
        if (null != element) {
            long limit = element.getAsLong();
            visitLimit(limit);
        }
        
        boolean isGroup = false;

        element = json.get("having");
        if (null != element) {
            visitHaving(element.getAsJsonObject());
        }

        element = json.get("groupBy");
        if (null != element) {
        	List<GroupItem> groups = groupsJsonArray2List(element.getAsJsonArray());
        	isGroup = !groups.isEmpty();
    		visitGroupBy(groups);
        }

        element = json.get("multiGroupBy");
        if (null != element) {
        	isGroup = true;
        	List<List<GroupItem>> groupsList = multiGroupsJsonArray2List(element.getAsJsonArray());
        	visitMultiGroupBy(groupsList);
        }

        visitIsCount(isCount(isGroup, fields));
    }
    
    private static boolean isCount(boolean isGroup, List<FieldItem> fields) {
    	if (isGroup) return false;
		List<String> funcFields = new ArrayList<>();
		if (fields == null || fields.isEmpty()) return false;
		fields.forEach(f -> {
			if (f.name().contains("(") && f.name().contains(")")) {
				funcFields.add(f.name());
			}
		});
		if (funcFields.size() == 1 && funcFields.get(0).startsWith("COUNT"))
			return true;
		return false;
    }

    private static List<FieldItem> fieldsJsonArray2List(JsonArray array) {
        List<FieldItem> fields = new ArrayList<>();
        if (null == array) return fields;
        for (JsonElement element : array) {
            fields.add(FieldItem.of(element.getAsJsonObject()));
        }
        return fields;
    }

    private static List<OrderItem> ordersJsonArray2List(JsonArray array) {
        List<OrderItem> orders = new ArrayList<>();
        if (null == array) return orders;
        for (JsonElement element : array) {
            orders.add(OrderItem.of(element.getAsJsonObject()));
        }
        return orders;
    }
    
    private static List<GroupItem> groupsJsonArray2List(JsonArray array) {
        List<GroupItem> groups = new ArrayList<>();
        if (null == array) return groups;
        for (JsonElement element : array) {
        	groups.add(GroupItem.of(element.getAsString()));
        }
        return groups;
    }
    
    private static List<List<GroupItem>> multiGroupsJsonArray2List(JsonArray array) {
    	List<List<GroupItem>> groupsList = new ArrayList<>();
        if (null == array) return groupsList;
        for (JsonElement element : array) {
        	List<GroupItem> groups = new ArrayList<>();
        	String fields = element.getAsString();
        	if (fields.contains(",")) {
        		for (String f : fields.split(",")) {
        			groups.add(GroupItem.of(f));
        		}
        	} else groups.add(GroupItem.of(fields));
        	groupsList.add(groups);
        }
        return groupsList;
    }
}
