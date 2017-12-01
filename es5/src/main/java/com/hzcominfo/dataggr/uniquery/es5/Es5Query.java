package com.hzcominfo.dataggr.uniquery.es5;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.ArrayList;
import java.util.List;

public interface Es5Query {

    QueryBuilder toEs5Query();

    static QueryBuilder of(JsonObject json) {
        if (null == json || 0 == json.size()) return new UnrecognizedEs5Query().toEs5Query();
        JsonElement element;
        JsonObject object, jl, jr;
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
                    field = json.get(key).getAsString();
                    return "is_null".equals(key) ? new IsNullEs5Query(field).toEs5Query() : new IsNotNullEs5Query(field).toEs5Query();
                case "equals":
                case "not_equals":
                case "greater_than":
                case "greater_than_or_equal":
                case "less_than":
                case "less_than_or_equal":
                case "like":
                case "not_like":
                    object = json.getAsJsonObject(key);
                    field = new ArrayList<>(object.keySet()).get(0);
                    element = object.get(field);
                    Object value = valueOf(element); // 只可能是基本类型
                    switch (key) {
                        case "equals":
                            return new EqualsEs5Query(field, value).toEs5Query();
                        case "not_equals":
                            return new NotEqualsEs5Query(field, value).toEs5Query();
                        case "greater_than":
                            return new GtEs5Query(field, value).toEs5Query();
                        case "greater_than_or_equal":
                            return new GeEs5Query(field, value).toEs5Query();
                        case "less_than":
                            return new LtEs5Query(field, value).toEs5Query();
                        case "less_than_or_equal":
                            return new LeEs5Query(field, value).toEs5Query();
                        case "like":
                            return new LikeEs5Query(field, value).toEs5Query();
                        case "not_like":
                            return new NotLikeEs5Query(field, value).toEs5Query();
                    }
                case "between":
                case "not_between":
                    object = json.getAsJsonObject(key);
                    field = new ArrayList<>(object.keySet()).get(0);
                    List<Object> se = new ArrayList<>();
                    object.get(field).getAsJsonArray().forEach(e -> se.add(e.getAsString()));
                    return "between".equals(key) ? new BetweenEs5Query(field, se.get(0), se.get(1)).toEs5Query()
                            : new NotBetweenEs5Query(field, se.get(0), se.get(1)).toEs5Query();
                case "in":
                case "not_in":
                    object = json.getAsJsonObject(key);
                    field = new ArrayList<>(object.keySet()).get(0);
                    List<Object> values = new ArrayList<>();
                    object.get(field).getAsJsonArray().forEach(e -> values.add(e.getAsString()));
                    return "in".equals(key) ? new InEs5Query(field, values).toEs5Query()
                            : new NotInEs5Query(field, values).toEs5Query();
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

        AndEs5Query(QueryBuilder left, QueryBuilder right) {
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

        OrEs5Query(QueryBuilder left, QueryBuilder right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public QueryBuilder toEs5Query() {
            return QueryBuilders.boolQuery().should(left).should(right).minimumShouldMatch(1);
        }
    }

    class EqualsEs5Query implements Es5Query {
        private String field;
        private Object value;

        EqualsEs5Query(String field, Object value) {
            this.field = field;
            this.value = value;
        }

        @Override
        public QueryBuilder toEs5Query() {
            return QueryBuilders.termQuery(field, value);
        }
    }

    class NotEqualsEs5Query implements Es5Query {
        private String field;
        private Object value;

        NotEqualsEs5Query(String field, Object value) {
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

        LtEs5Query(String field, Object value) {
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

        LeEs5Query(String field, Object value) {
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

        GtEs5Query(String field, Object value) {
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

        GeEs5Query(String field, Object value) {
            this.field = field;
            this.value = value;
        }

        @Override
        public QueryBuilder toEs5Query() {
            return QueryBuilders.rangeQuery(field).to(value, true);
        }
    }

    class IsNullEs5Query implements Es5Query {
        private String field;

        public IsNullEs5Query(String field) {
            this.field = field;
        }

        /**
         * see https://www.elastic.co/guide/en/elasticsearch/reference/5.5/null-value.html
         */
        @Override
        public QueryBuilder toEs5Query() {
            return QueryBuilders.termQuery(field, "NULL");
        }
    }

    class IsNotNullEs5Query implements Es5Query {
        private String field;

        IsNotNullEs5Query(String field) {
            this.field = field;
        }

        /**
         * see https://www.elastic.co/guide/en/elasticsearch/reference/5.5/null-value.html
         */
        @Override
        public QueryBuilder toEs5Query() {
            return QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery(field, "NULL"));
        }
    }

    /**
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-wildcard-query.html
     */
    class LikeEs5Query implements Es5Query {
        private String field;
        private Object value;

        LikeEs5Query(String field, Object value) {
            this.field = field;
            this.value = value;
        }

        @Override
        public QueryBuilder toEs5Query() {
            return QueryBuilders.wildcardQuery(field,
                    value.toString().replaceAll("%", "*").replaceAll("_", "?"));
        }
    }

    /**
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-wildcard-query.html
     */
    class NotLikeEs5Query implements Es5Query {
        private String field;
        private Object value;

        NotLikeEs5Query(String field, Object value) {
            this.field = field;
            this.value = value;
        }

        @Override
        public QueryBuilder toEs5Query() {
            return QueryBuilders.boolQuery().mustNot(QueryBuilders.wildcardQuery(field,
                    value.toString().replaceAll("%", "*").replaceAll("_", "?")));
        }
    }

    /**
     * 与solr实现保持一致，包含边界
     */
    class BetweenEs5Query implements Es5Query {
        private String field;
        private Object start, end;

        BetweenEs5Query(String field, Object start, Object end) {
            this.field = field;
            this.start = start;
            this.end = end;
        }

        @Override
        public QueryBuilder toEs5Query() {
            return QueryBuilders.rangeQuery(field).from(start, true).to(end, true);
        }
    }

    class NotBetweenEs5Query implements Es5Query {
        private String field;
        private Object start, end;

        NotBetweenEs5Query(String field, Object start, Object end) {
            this.field = field;
            this.start = start;
            this.end = end;
        }

        @Override
        public QueryBuilder toEs5Query() {
            return QueryBuilders.boolQuery().mustNot(
                    QueryBuilders.rangeQuery(field).from(start, true).to(end, true));
        }
    }

    class InEs5Query implements Es5Query {
        private String field;
        private List<Object> values;

        InEs5Query(String field, List<Object> values) {
            assert values != null;
            this.field = field;
            this.values = values;
        }

        @Override
        public QueryBuilder toEs5Query() {
            BoolQueryBuilder builder = QueryBuilders.boolQuery();
            values.forEach(obj -> builder.should(QueryBuilders.termQuery(field, obj)));
            builder.minimumShouldMatch(1);
            return builder;
        }
    }

    class NotInEs5Query implements Es5Query {
        private String field;
        private List<Object> values;

        NotInEs5Query(String field, List<Object> values) {
            assert values != null;
            this.field = field;
            this.values = values;
        }

        @Override
        public QueryBuilder toEs5Query() {
            BoolQueryBuilder builder = QueryBuilders.boolQuery();
            values.forEach(obj -> builder.mustNot(QueryBuilders.termQuery(field, obj)));
            return builder;
        }
    }

}
