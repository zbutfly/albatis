package com.hzcominfo.dataggr.uniquery;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class FieldItem {
    private List<String> names;
    private String alias;

    private FieldItem(String field, String alias) {
        names = new ArrayList<>();
        names.addAll(Arrays.asList(field.split("\\.")));
        this.alias = alias;
    }

    public static FieldItem of(JsonObject json) {
        JsonElement element;
        element = json.get("alias");
        String alias = null == element ? null : element.getAsString();
        element = json.get("field");
        String field;
        if (null == element) throw new IllegalArgumentException(json.toString() + " has no ```field```");
        field = element.getAsString();

        return new FieldItem(field, alias);
    }

    public String name() {
        return names.get(names.size() - 1);
    }

    public String alias() {
        return alias;
    }

    public String full() {
        return names.stream().collect(Collectors.joining("."));
    }

    @Override
    public String toString() {
        return full() + (null == alias ? "" : (" AS " + alias));
    }
}
