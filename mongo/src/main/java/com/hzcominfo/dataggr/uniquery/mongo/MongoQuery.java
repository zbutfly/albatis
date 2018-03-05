package com.hzcominfo.dataggr.uniquery.mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import net.butfly.albacore.utils.Pair;

import java.util.ArrayList;
import java.util.List;

public class MongoQuery {
    private DBObject query;
    private DBObject fields;
    private DBObject sort;
    private DBObject pipelineGroupId;
    private List<Pair<String, DBObject>> pipelineGroupAggItem;
    private int offset;
    private int limit;
    private List<DBObject> pipeline;
    private boolean aggr;

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

    public DBObject getSort() {
        return sort;
    }

    public void setSort(DBObject sort) {
        this.sort = sort;
    }

    public void setPipelineGroupId(DBObject pipelineGroupId) {
        this.pipelineGroupId = pipelineGroupId;
    }

    public void setPipelineGroupAggItem(List<Pair<String, DBObject>> pipelineGroupAggItem) {
        if (null == this.pipelineGroupAggItem)
            this.pipelineGroupAggItem = pipelineGroupAggItem;
        else
            this.pipelineGroupAggItem.addAll(pipelineGroupAggItem);
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

    public List<DBObject> getPipeline() {
        if (null != pipeline) return pipeline;
        pipeline = new ArrayList<>();
        pipeline.add(new BasicDBObject("$match", query));
        if (null != sort && sort.keySet().size() > 0) pipeline.add(new BasicDBObject("$sort", sort));
        pipeline.add(new BasicDBObject("$skip", offset));
        pipeline.add(new BasicDBObject("$limit", limit));
        BasicDBObject group = new BasicDBObject();
        group.append("_id", pipelineGroupId);
        pipelineGroupAggItem.forEach(pair -> group.append(pair.v1(), pair.v2()));
        pipeline.add(new BasicDBObject("$group", group));
        return pipeline;
    }

	public boolean isAggr() {
		return aggr;
	}

	public void setAggr(boolean aggr) {
		this.aggr = aggr;
	}
}
