package com.hzcominfo.dataggr.uniquery;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class TableItem {
    private List<String> names;
    private String alias;

    private TableItem(JsonObject json) {
        names = new ArrayList<>();
        JsonElement element;
        element = json.get("table");
        if (null == element) throw new IllegalArgumentException(json.toString() + " has no ```table```");
        names.addAll(Arrays.asList(element.getAsString().split("\\.")));
        element = json.get("alias");
        alias = null == element ? null : element.getAsString();
    }

    public TableItem of(JsonObject json) {
        return new TableItem(json);
    }

    public String name() {
        return names.get(names.size() - 1);
    }

    public String name(int last) {
        if (last >= name().length()) return null;
        return names.get(names.size() - last - 1);
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
