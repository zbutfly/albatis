package com.hzcominfo.albatis.nosql.search;

import java.util.Collection;

public class CalculusBuild {
    public static Criteria and(Criteria... filter) {
        return new Criteria.And(filter);
    }

    public static Criteria or(Criteria... filter) {
        return new Criteria.Or(filter);
    }

    public static Criteria not(Criteria... filter) {
        return new Criteria.Not(filter);
    }

    public static <V> Criteria equal(String field, V value) {
        return new Criteria.Equal<V>(field, value);
    }

    public static <V> Criteria notEqual(String field, V value) {
        return new Criteria.NotEqual<V>(field, value);
    }

    public static <V> Criteria less(String field, V value) {
        return new Criteria.Less<V>(field, value);
    }

    public static <V> Criteria greater(String field, V value) {
        return new Criteria.Greater<V>(field, value);
    }

    public static <V> Criteria lessOrEqual(String field, V value) {
        return new Criteria.LessOrEqual<V>(field, value);
    }

    public static <V> Criteria greaterOrEqual(String field, V value) {
        return new Criteria.GreaterOrEqual<V>(field, value);
    }

    public static <V> Criteria In(String field, Collection<V> values) {
        return new Criteria.In<V>(field, values);
    }

    @SafeVarargs
    public static <V> Criteria In(String field, V... value) {
        return new Criteria.In<V>(field, value);
    }

    public static <V> Criteria random(float chance) {
        return new Criteria.Random(chance);
    }

    public static <V> Criteria regex(String field, String regex) {
        return new Criteria.Regex(field, regex);
    }

    public static <V> Criteria allField(V value) {
        return new Criteria.AllField<V>(value);
    }

    public static OrderBy sort(String field, OrderBy.Order order) {
        return new OrderBy.Sort(field, order);
    }

    public static DataFrom db(String dbName) {
        return new DataFrom.DB(dbName);
    }

    public static DataFrom table(String tableName) {
        return new DataFrom.Table(tableName);
    }

    public static DataFrom table(String tableName, String dbName) {
        return new DataFrom.Table(tableName).setDBName(dbName);
    }

    public static SearchItem column(String columnName) {
        return new SearchItem.Column(columnName);
    }

    public static SearchItem sum(String columnName) {
        return new SearchItem.Sum(columnName);
    }

    public static SearchItem count(String columnName) {
        return new SearchItem.Count(columnName);
    }

    public static SearchItem max(String columnName) {
        return new SearchItem.Max(columnName);
    }

    public static SearchItem min(String columnName) {
        return new SearchItem.Min(columnName);
    }

    public static SearchItem add(String fristColumn, String secondColumn) {
        return new SearchItem.Add(fristColumn, secondColumn);
    }

    public static SearchItem sub(String fristColumn, String secondColumn) {
        return new SearchItem.Sub(fristColumn, secondColumn);
    }

}
