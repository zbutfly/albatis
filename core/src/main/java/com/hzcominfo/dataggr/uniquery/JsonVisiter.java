package com.hzcominfo.dataggr.uniquery;

import com.google.gson.JsonObject;

import java.util.List;

public interface JsonVisiter<V> {

//    V distinct(boolean distinct);

    void visitFields(List<FieldItem> fields, boolean distinct);

    void visitTables(List<TableItem> tables);

    void visitConditions(JsonObject json);

    void visitGroupBy(List<GroupItem> groups);
    
    void visitMultiGroupBy(List<List<GroupItem>> groupsList);

    void visitOrderBy(List<OrderItem> orders);

    void visitHaving();

    void visitOffset(long offset);

    void visitLimit(long limit);


    V get();
}
