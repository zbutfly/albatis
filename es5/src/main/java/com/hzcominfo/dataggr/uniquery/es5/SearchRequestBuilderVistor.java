package com.hzcominfo.dataggr.uniquery.es5;

import com.google.gson.JsonObject;
import com.hzcominfo.dataggr.uniquery.*;
import org.elasticsearch.action.search.SearchRequestBuilder;

import java.util.List;

public class SearchRequestBuilderVistor extends JsonBasicVisitor<SearchRequestBuilder> {

    public SearchRequestBuilderVistor(SearchRequestBuilder searchRequestBuilder, JsonObject json) {
        super(searchRequestBuilder, json);
    }

    @Override
    public void visitFields(List<FieldItem> fields, boolean distinct) {
        SearchRequestBuilder builder = super.get();
        builder.storedFields(fields.stream().map(FieldItem::name).toArray(String[]::new));
    }

    @Override
    public void visitTables(List<TableItem> tables) {

    }

    @Override
    public void visitConditions(JsonObject json) {

    }

    @Override
    public void visitGroupBy(List<GroupItem> groups) {

    }

    @Override
    public void visitMultiGroupBy(List<List<GroupItem>> groupsList) {

    }

    @Override
    public void visitOrderBy(List<OrderItem> orders) {

    }

    @Override
    public void visitHaving() {

    }

    @Override
    public void visitOffset(long offset) {

    }

    @Override
    public void visitLimit(long limit) {

    }
}
