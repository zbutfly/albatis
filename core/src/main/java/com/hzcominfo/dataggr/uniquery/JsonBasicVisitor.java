package com.hzcominfo.dataggr.uniquery;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.List;

public abstract class JsonBasicVisitor<V> implements JsonVisiter<V> {
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
        element = json.get("distinct");
        boolean distinct = null != element && element.getAsBoolean();
        List<FieldItem> fields = fieldsJsonArray2List(json.getAsJsonArray("fields"));
        visitFields(fields, distinct);
        // TODO: 2017/11/16 visit table

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
}
