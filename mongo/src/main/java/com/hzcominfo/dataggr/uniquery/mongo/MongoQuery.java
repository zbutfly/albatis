package com.hzcominfo.dataggr.uniquery.mongo;

import com.mongodb.DBObject;

public class MongoQuery {
    private DBObject query;
    private DBObject fields;
    private int offset;
    private int limit;

    public DBObject getQuery() {
        return query;
    }

    void setQuery(DBObject query) {
        this.query = query;
    }

    public DBObject getFields() {
        return fields;
    }

    void setFields(DBObject fields) {
        this.fields = fields;
    }

    public int getOffset() {
        return offset;
    }

    void setOffset(int offset) {
        this.offset = offset;
    }

    public int getLimit() {
        return limit;
    }

    void setLimit(int limit) {
        this.limit = limit;
    }
}
