package com.hzcominfo.albatis.nosql.search.result;

import net.butfly.albacore.exception.NotImplementedException;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.hzcominfo.albatis.nosql.search.exception.SearchAPIException;

/**
 * 结果集的描述类，一般用于直接数据返回的时候可以忽略
 */
public interface ResultMetadata extends ResultSetMetaData {

    @Override
    default int getColumnCount() throws SQLException {
        throw new SearchAPIException(new NotImplementedException());
    }

    @Override
    default boolean isAutoIncrement(int column) throws SQLException {
        throw new SearchAPIException(new NotImplementedException());
    }

    @Override
    default boolean isCaseSensitive(int column) throws SQLException {
        throw new SearchAPIException(new NotImplementedException());
    }

    @Override
    default boolean isSearchable(int column) throws SQLException {
        throw new SearchAPIException(new NotImplementedException());
    }

    @Override
    default boolean isCurrency(int column) throws SQLException {
        throw new SearchAPIException(new NotImplementedException());
    }

    @Override
    default int isNullable(int column) throws SQLException {
        throw new SearchAPIException(new NotImplementedException());
    }

    @Override
    default boolean isSigned(int column) throws SQLException {
        throw new SearchAPIException(new NotImplementedException());
    }

    @Override
    default int getColumnDisplaySize(int column) throws SQLException {
        throw new SearchAPIException(new NotImplementedException());
    }

    @Override
    default String getColumnLabel(int column) throws SQLException {
        throw new SearchAPIException(new NotImplementedException());
    }

    @Override
    default String getColumnName(int column) throws SQLException {
        throw new SearchAPIException(new NotImplementedException());
    }

    @Override
    default String getSchemaName(int column) throws SQLException {
        throw new SearchAPIException(new NotImplementedException());
    }

    @Override
    default int getPrecision(int column) throws SQLException {
        throw new SearchAPIException(new NotImplementedException());
    }

    @Override
    default int getScale(int column) throws SQLException {
        throw new SearchAPIException(new NotImplementedException());
    }

    @Override
    default String getTableName(int column) throws SQLException {
        throw new SearchAPIException(new NotImplementedException());
    }

    @Override
    default String getCatalogName(int column) throws SQLException {
        throw new SearchAPIException(new NotImplementedException());
    }

    @Override
    default int getColumnType(int column) throws SQLException {
        throw new SearchAPIException(new NotImplementedException());
    }

    @Override
    default String getColumnTypeName(int column) throws SQLException {
        throw new SearchAPIException(new NotImplementedException());
    }

    @Override
    default boolean isReadOnly(int column) throws SQLException {
        throw new SearchAPIException(new NotImplementedException());
    }

    @Override
    default boolean isWritable(int column) throws SQLException {
        throw new SearchAPIException(new NotImplementedException());
    }

    @Override
    default boolean isDefinitelyWritable(int column) throws SQLException {
        throw new SearchAPIException(new NotImplementedException());
    }

    @Override
    default String getColumnClassName(int column) throws SQLException {
        throw new SearchAPIException(new NotImplementedException());
    }

    @Override
    default <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SearchAPIException(new NotImplementedException());
    }

    @Override
    default boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SearchAPIException(new NotImplementedException());
    }
}
