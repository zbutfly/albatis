package com.hzcominfo.dataggr.uniquery;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class AggItem {
    private String func;
    private String keyword;
    private String field;
    private String alias;

    private AggItem(String func, String keyword, String field, String alias) {
        this.func = func.toUpperCase();
        this.keyword = keyword;
        this.field = field;
        this.alias = alias;
    }

    public static AggItem of(String agg) {
        return of(agg, null);
    }

    public static AggItem of(String agg, String alias) {
        assert agg.contains("(") && agg.contains(")");
        String func = agg.substring(0, agg.indexOf("("));
        String[] params = agg.substring(agg.indexOf("(") + 1, agg.indexOf(")")).split(" ");
        String keyword = null;
        if (params.length >= 2 && !params[0].isEmpty()) {
            keyword = params[0];
        }
        String field = params[params.length - 1];
        return new AggItem(func.toUpperCase(), keyword, field, alias);
    }

    public static AggItem of(JsonObject json) {
        String field = json.get("field").getAsString();
        String alias = null;
        JsonElement element = json.get("alias");
        if (null != element) alias = element.getAsString();
        return of(field, alias);
    }

    public String function() {
        return func;
    }

    public String keyword() {
        return keyword;
    }

    public String field() {
        return field;
    }

    public String alias() {
        return alias;
    }

    public String name() {
        return null != alias ? alias : func + "(" + (null == keyword ? "" : keyword + " ") + field + ")";
    }

    @Override
    public String toString() {
        return func + "(" + (null == keyword ? "" : keyword + " ") + field + ")" + (null == alias ? "" : " AS " + alias);
    }

    public static void main(String[] args) {
        AggItem item = AggItem.of("count(distinct name)", "cnt");
        System.out.println(item);
        System.out.println(item.name());
        item = AggItem.of("count(name)", "cnt");
        System.out.println(item);
        System.out.println(item.name());
    }
}
