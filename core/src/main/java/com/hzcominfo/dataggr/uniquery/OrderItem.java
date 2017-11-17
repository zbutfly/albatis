package com.hzcominfo.dataggr.uniquery;

import com.google.gson.JsonObject;

public class OrderItem {
    private String field;
    private boolean desc;

    private OrderItem(JsonObject json) {
        assert null != json && json.keySet().size() == 1;
        json.entrySet().forEach(entry -> {
            field = entry.getKey();
            desc = "DESC".equalsIgnoreCase(entry.getValue().getAsString());
        });
    }

    public static OrderItem of(JsonObject json) {
        return new OrderItem(json);
    }

    public String name() {
        return field;
    }

    public boolean desc() {
        return desc;
    }

    @Override
    public String toString() {
        return field + (desc ? " DESC" : " ASC");
    }
}
