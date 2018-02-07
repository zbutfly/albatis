package com.hzcominfo.dataggr.uniquery.mongo;

import com.google.gson.JsonObject;
import com.hzcominfo.dataggr.uniquery.*;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import java.util.List;

public class MongoQueryVisitor extends JsonBasicVisitor<MongoQuery> {

    public MongoQueryVisitor(MongoQuery mongoQuery, JsonObject json) {
        super(mongoQuery, json);
        visit(json);
    }

    @Override
    public void visitFields(List<FieldItem> fields, boolean distinct) {
        DBObject fieldss = new BasicDBObject();
        if (fields.stream().map(FieldItem::name).anyMatch("*"::equals)) {
            get().setFields(fieldss);
            return;
        }
        fields.forEach(item -> fieldss.put(item.name(), 1));
        get().setFields(fieldss);
    }

    @Override
    public void visitTables(List<TableItem> tables) {
        // do nothing
    }

    @Override
    public void visitConditions(JsonObject json) {
        get().setQuery(MongoConditionTransverter.of(json));
    }

    @Override
    public void visitGroupBy(List<GroupItem> groups) {
        // mongo group by
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
        get().setOffset((int) offset);
    }

    @Override
    public void visitLimit(long limit) {
        get().setLimit((int) limit);
    }

    @Override
    public void visitIsCount(boolean idGroup) {

    }
}
