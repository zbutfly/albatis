package com.hzcominfo.dataggr.uniquery.es5;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.ArrayList;

public interface Es5Query {

    QueryBuilder toEs5Query();

    static QueryBuilder of(JsonObject json) {
        if (null == json || 0 == json.size()) return new UnrecognizedEs5Query().toEs5Query();
        JsonElement element;
        JsonObject object, jl, jr;
        QueryBuilder ql, qr;
        JsonArray array;
        String field;
        for (String key : json.keySet()) {
            switch (key) {
                case "and":
                case "or":
                    array = json.getAsJsonArray(key);
                    jl = array.get(0).getAsJsonObject();
                    jr = array.get(1).getAsJsonObject();
                    return "and".equals(key) ? new AndEs5Query(of(jl), of(jr)).toEs5Query() : new OrEs5Query(of(jl), of(jr)).toEs5Query();
                case "is_null":
                case "is_not_null":
//                    field = json.get(key).getAsString();
//                    return "is_null".equals(key) ? new IsNullSolrFilter(field) : new IsNotNullSolrFilter(field);
                case "equals":
//                    QueryBuilders.termQuery()
                    // "where":{"equals":{"a":"b"}}
                    object = json.getAsJsonObject(key);
                    field = new ArrayList<>(object.keySet()).get(0);
                    element = object.get(field);
                    Object value = valueOf(element);
                    return QueryBuilders.termQuery(field, value);
                case "not_equals":
                case "greater_than":
                case "greater_than_or_equal":
                case "less_than":
                case "less_than_or_equal":
                case "like":
                case "not_like":
//                    object = json.getAsJsonObject(key);
//                    field = new ArrayList<>(object.keySet()).get(0);
//                    element = object.get(field);
//                    Object value = valueOf(element); // 只可能是基本类型
//                    switch (key) {
//                        case "equals":
//                            return new EqualsSolrFilter(field, value);
//                        case "not_equals":
//                            return new NotEqualsSolrFilter(field, value);
//                        case "greater_than":
//                            return new GtSolrFilter(field, value);
//                        case "greater_than_or_equal":
//                            return new GeSolrFilter(field, value);
//                        case "less_than":
//                            return new LtSolrFilter(field, value);
//                        case "less_than_or_equal":
//                            return new LeSolrFilter(field, value);
//                        case "like":
//                            return new LikeSolrFilter(field, value);
//                        case "not_like":
//                            return new NotLikeSolrFilter(field, value);

//                    }
                case "between":
                case "not_between":
//                    object = json.getAsJsonObject(key);
//                    field = new ArrayList<>(object.keySet()).get(0);
//                    List<Object> se = new ArrayList<>();
//                    object.get(field).getAsJsonArray().forEach(e -> se.add(e.getAsString()));
//                    return "between".equals(key) ? new BetweenSolrFilter(field, se.get(0), se.get(1)) : new NotBetweenSolrFilter(field, se.get(0), se.get(1));
                case "in":
                case "not_in":
//                    object = json.getAsJsonObject(key);
//                    field = new ArrayList<>(object.keySet()).get(0);
//                    List<Object> values = new ArrayList<>();
//                    object.get(field).getAsJsonArray().forEach(e -> values.add(e.getAsString()));
//                    return "in".equals(key) ? new InSolrFilter(field, values) : new NotInSolrFilter(field, values);
            }
        }


        throw new RuntimeException("Can NOT parse " + json + "to Es5 Query");
    }

    static Object valueOf(JsonElement element) {
        if (null == element || element.isJsonNull()) return null;
        if (element.isJsonPrimitive()) {
            JsonPrimitive primitive = element.getAsJsonPrimitive();
            if (primitive.isString()) return primitive.getAsString();
            if (primitive.isBoolean()) return primitive.getAsBoolean();
            if (primitive.isNumber()) return primitive.getAsNumber();
        }
        return element;
    }

    class UnrecognizedEs5Query implements Es5Query {

        @Override
        public QueryBuilder toEs5Query() {
            return QueryBuilders.matchAllQuery();
        }
    }

    class AndEs5Query implements Es5Query {
        private QueryBuilder left, right;

        public AndEs5Query(QueryBuilder left, QueryBuilder right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public QueryBuilder toEs5Query() {
            return QueryBuilders.boolQuery().must(left).must(right);
        }
    }

    class OrEs5Query implements Es5Query {
        private QueryBuilder left, right;

        public OrEs5Query(QueryBuilder left, QueryBuilder right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public QueryBuilder toEs5Query() {
            return QueryBuilders.boolQuery().should(left).should(right).minimumShouldMatch(1);
        }
    }

    class EqualEs5Query implements Es5Query {
        private String field;
        private Object value;

        public EqualEs5Query(String field, Object value) {
            this.field = field;
            this.value = value;
        }

        @Override
        public QueryBuilder toEs5Query() {
            return QueryBuilders.termQuery(field, value);
        }
    }

    class NotEqualEs5Query implements Es5Query {
        private String field;
        private Object value;

        public NotEqualEs5Query(String field, Object value) {
            this.field = field;
            this.value = value;
        }

        @Override
        public QueryBuilder toEs5Query() {
            return QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery(field, value));
        }
    }

    class LtEs5Query implements Es5Query {
        private String field;
        private Object value;

        public LtEs5Query(String field, Object value) {
            this.field = field;
            this.value = value;
        }

        @Override
        public QueryBuilder toEs5Query() {
            return QueryBuilders.rangeQuery(field).from(value, false);
        }
    }

    class LeEs5Query implements Es5Query {
        private String field;
        private Object value;

        public LeEs5Query(String field, Object value) {
            this.field = field;
            this.value = value;
        }

        @Override
        public QueryBuilder toEs5Query() {
            return QueryBuilders.rangeQuery(field).from(value, true);
        }
    }

    class GtEs5Query implements Es5Query {
        private String field;
        private Object value;

        public GtEs5Query(String field, Object value) {
            this.field = field;
            this.value = value;
        }

        @Override
        public QueryBuilder toEs5Query() {
            return QueryBuilders.rangeQuery(field).to(value, false);
        }
    }

    class GeEs5Query implements Es5Query {
        private String field;
        private Object value;

        public GeEs5Query(String field, Object value) {
            this.field = field;
            this.value = value;
        }

        @Override
        public QueryBuilder toEs5Query() {
            return QueryBuilders.rangeQuery(field).to(value, true);
        }
    }

}
