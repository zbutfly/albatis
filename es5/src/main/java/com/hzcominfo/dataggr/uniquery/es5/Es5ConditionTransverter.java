package com.hzcominfo.dataggr.uniquery.es5;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.hzcominfo.dataggr.uniquery.ConditionTransverter;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.ArrayList;
import java.util.List;

import static com.hzcominfo.dataggr.uniquery.ConditionTransverter.valueOf;

public interface Es5ConditionTransverter extends ConditionTransverter {
    String KEYWORD_SUFFIX = ".keyword";

    QueryBuilder toEs5Query();

    static QueryBuilder of(JsonObject json) {
        if (null == json || 0 == json.size()) return new UnrecognizedEs5ConditionTransverter().toEs5Query();
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
                    return "and".equals(key) ? new AndEs5ConditionTransverter(of(jl), of(jr)).toEs5Query() : new OrEs5ConditionTransverter(of(jl), of(jr)).toEs5Query();
                case "is_null":
                case "is_not_null":
                    field = json.get(key).getAsString();
                    return "is_null".equals(key) ? new IsNullEs5ConditionTransverter(field).toEs5Query() : new IsNotNullEs5ConditionTransverter(field).toEs5Query();
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
                            return new EqualsEs5ConditionTransverter(field, value).toEs5Query();
                        case "not_equals":
                            return new NotEqualsEs5ConditionTransverter(field, value).toEs5Query();
                        case "greater_than":
                            return new GtEs5ConditionTransverter(field, value).toEs5Query();
                        case "greater_than_or_equal":
                            return new GeEs5ConditionTransverter(field, value).toEs5Query();
                        case "less_than":
                            return new LtEs5ConditionTransverter(field, value).toEs5Query();
                        case "less_than_or_equal":
                            return new LeEs5ConditionTransverter(field, value).toEs5Query();
                        case "like":
                            return new LikeEs5ConditionTransverter(field, value).toEs5Query();
                        case "not_like":
                            return new NotLikeEs5ConditionTransverter(field, value).toEs5Query();
                    }
                case "between":
                case "not_between":
                    object = json.getAsJsonObject(key);
                    field = new ArrayList<>(object.keySet()).get(0);
                    List<Object> se = new ArrayList<>();
                    object.get(field).getAsJsonArray().forEach(e -> se.add(e.getAsString()));
                    return "between".equals(key) ? new BetweenEs5ConditionTransverter(field, se.get(0), se.get(1)).toEs5Query()
                            : new NotBetweenEs5ConditionTransverter(field, se.get(0), se.get(1)).toEs5Query();
                case "in":
                case "not_in":
                    object = json.getAsJsonObject(key);
                    field = new ArrayList<>(object.keySet()).get(0);
                    List<Object> values = new ArrayList<>();
                    object.get(field).getAsJsonArray().forEach(e -> values.add(e.getAsString()));
                    return "in".equals(key) ? new InEs5ConditionTransverter(field, values).toEs5Query()
                            : new NotInEs5ConditionTransverter(field, values).toEs5Query();
            }
        }
        throw new RuntimeException("Can NOT parse " + json + "to Es5 Query");
    }

    class UnrecognizedEs5ConditionTransverter implements Es5ConditionTransverter {

        @Override
        public QueryBuilder toEs5Query() {
            return QueryBuilders.matchAllQuery();
        }
    }

    class AndEs5ConditionTransverter implements Es5ConditionTransverter {
        private QueryBuilder left, right;

        AndEs5ConditionTransverter(QueryBuilder left, QueryBuilder right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public QueryBuilder toEs5Query() {
            return QueryBuilders.boolQuery().must(left).must(right);
        }
    }

    class OrEs5ConditionTransverter implements Es5ConditionTransverter {
        private QueryBuilder left, right;

        OrEs5ConditionTransverter(QueryBuilder left, QueryBuilder right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public QueryBuilder toEs5Query() {
            return QueryBuilders.boolQuery().should(left).should(right).minimumShouldMatch(1);
        }
    }

    class EqualsEs5ConditionTransverter implements Es5ConditionTransverter {
        private String field;
        private Object value;

        EqualsEs5ConditionTransverter(String field, Object value) {
            this.field = field + KEYWORD_SUFFIX;
            this.value = value;
        }

        @Override
        public QueryBuilder toEs5Query() {
            return QueryBuilders.termQuery(field, value);
        }
    }

    class NotEqualsEs5ConditionTransverter implements Es5ConditionTransverter {
        private String field;
        private Object value;

        NotEqualsEs5ConditionTransverter(String field, Object value) {
            this.field = field + KEYWORD_SUFFIX;
            this.value = value;
        }

        @Override
        public QueryBuilder toEs5Query() {
            return QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery(field, value));
        }
    }

    class LtEs5ConditionTransverter implements Es5ConditionTransverter {
        private String field;
        private Object value;

        LtEs5ConditionTransverter(String field, Object value) {
            this.field = field + KEYWORD_SUFFIX;
            this.value = value;
        }

        @Override
        public QueryBuilder toEs5Query() {
            return QueryBuilders.rangeQuery(field).to(value, false);
        }
    }

    class LeEs5ConditionTransverter implements Es5ConditionTransverter {
        private String field;
        private Object value;

        LeEs5ConditionTransverter(String field, Object value) {
            this.field = field + KEYWORD_SUFFIX;
            this.value = value;
        }

        @Override
        public QueryBuilder toEs5Query() {
            return QueryBuilders.rangeQuery(field).to(value, true);
        }
    }

    class GtEs5ConditionTransverter implements Es5ConditionTransverter {
        private String field;
        private Object value;

        GtEs5ConditionTransverter(String field, Object value) {
            this.field = field + KEYWORD_SUFFIX;
            this.value = value;
        }

        @Override
        public QueryBuilder toEs5Query() {
            return QueryBuilders.rangeQuery(field).from(value, false);
        }
    }

    class GeEs5ConditionTransverter implements Es5ConditionTransverter {
        private String field;
        private Object value;

        GeEs5ConditionTransverter(String field, Object value) {
            this.field = field + KEYWORD_SUFFIX;
            this.value = value;
        }

        @Override
        public QueryBuilder toEs5Query() {
            return QueryBuilders.rangeQuery(field).from(value, true);
        }
    }

    class IsNullEs5ConditionTransverter implements Es5ConditionTransverter {
        private String field;

        IsNullEs5ConditionTransverter(String field) {
            this.field = field + KEYWORD_SUFFIX;
        }

        /**
         * see https://www.elastic.co/guide/en/elasticsearch/reference/5.5/null-value.html
         */
        @Override
        public QueryBuilder toEs5Query() {
            return QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(field));
        }
    }

    class IsNotNullEs5ConditionTransverter implements Es5ConditionTransverter {
        private String field;

        IsNotNullEs5ConditionTransverter(String field) {
            this.field = field + KEYWORD_SUFFIX;
        }

        /**
         * see https://www.elastic.co/guide/en/elasticsearch/reference/5.5/null-value.html
         * https://www.elastic.co/guide/en/elasticsearch/reference/5.5/query-dsl-exists-query.html#_literal_missing_literal_query
         */
        @Override
        public QueryBuilder toEs5Query() {
            return QueryBuilders.boolQuery().must(QueryBuilders.existsQuery(field));
        }
    }

    /**
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-wildcard-query.html
     * https://www.elastic.co/guide/en/elasticsearch/reference/5.5/query-dsl-exists-query.html#_literal_missing_literal_query
     */
    class LikeEs5ConditionTransverter implements Es5ConditionTransverter {
        private String field;
        private Object value;

        LikeEs5ConditionTransverter(String field, Object value) {
            this.field = field + KEYWORD_SUFFIX;
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
    class NotLikeEs5ConditionTransverter implements Es5ConditionTransverter {
        private String field;
        private Object value;

        NotLikeEs5ConditionTransverter(String field, Object value) {
            this.field = field + KEYWORD_SUFFIX;
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
    class BetweenEs5ConditionTransverter implements Es5ConditionTransverter {
        private String field;
        private Object start, end;

        BetweenEs5ConditionTransverter(String field, Object start, Object end) {
            this.field = field + KEYWORD_SUFFIX;
            this.start = start;
            this.end = end;
        }

        @Override
        public QueryBuilder toEs5Query() {
            return QueryBuilders.rangeQuery(field).from(start, true).to(end, true);
        }
    }

    class NotBetweenEs5ConditionTransverter implements Es5ConditionTransverter {
        private String field;
        private Object start, end;

        NotBetweenEs5ConditionTransverter(String field, Object start, Object end) {
            this.field = field + KEYWORD_SUFFIX;
            this.start = start;
            this.end = end;
        }

        @Override
        public QueryBuilder toEs5Query() {
            return QueryBuilders.boolQuery().mustNot(
                    QueryBuilders.rangeQuery(field).from(start, true).to(end, true));
        }
    }

    class InEs5ConditionTransverter implements Es5ConditionTransverter {
        private String field;
        private List<Object> values;

        InEs5ConditionTransverter(String field, List<Object> values) {
            assert values != null;
            this.field = field + KEYWORD_SUFFIX;
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

    class NotInEs5ConditionTransverter implements Es5ConditionTransverter {
        private String field;
        private List<Object> values;

        NotInEs5ConditionTransverter(String field, List<Object> values) {
            assert values != null;
            this.field = field + KEYWORD_SUFFIX;
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
