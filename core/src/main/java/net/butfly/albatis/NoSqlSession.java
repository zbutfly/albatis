//package net.butfly.albatis;
//
//import java.sql.Connection;
//import java.util.List;
//import java.util.Map;
//
//import org.apache.ibatis.executor.BatchResult;
//import org.apache.ibatis.session.Configuration;
//import org.apache.ibatis.session.ResultHandler;
//import org.apache.ibatis.session.RowBounds;
//import org.apache.ibatis.session.SqlSession;
//
//public interface NoSqlSession extends SqlSession {
//	@Override
//	<T> T selectOne(String statement);
//
//	@Override
//	<T> T selectOne(String statement, Object parameter);
//
//	@Override
//	<E> List<E> selectList(String statement);
//
//	@Override
//	<E> List<E> selectList(String statement, Object parameter);
//
//	@Override
//	<E> List<E> selectList(String statement, Object parameter, RowBounds rowBounds);
//
//	@Override
//	<K, V> Map<K, V> selectMap(String statement, String mapKey);
//
//	@Override
//	<K, V> Map<K, V> selectMap(String statement, Object parameter, String mapKey);
//
//	@Override
//	<K, V> Map<K, V> selectMap(String statement, Object parameter, String mapKey, RowBounds rowBounds);
//
//	@Override
//	void select(String statement, Object parameter, @SuppressWarnings("rawtypes") ResultHandler handler);
//
//	@Override
//	void select(String statement, @SuppressWarnings("rawtypes") ResultHandler handler);
//
//	@Override
//	void select(String statement, Object parameter, RowBounds rowBounds, @SuppressWarnings("rawtypes") ResultHandler handler);
//
//	@Override
//	int insert(String statement);
//
//	@Override
//	int insert(String statement, Object parameter);
//
//	@Override
//	int update(String statement);
//
//	@Override
//	int update(String statement, Object parameter);
//
//	@Override
//	int delete(String statement);
//
//	@Override
//	int delete(String statement, Object parameter);
//
//	@Override
//	List<BatchResult> flushStatements();
//
//	@Override
//	void clearCache();
//
//	@Override
//	Configuration getConfiguration();
//
//	@Override
//	<T> T getMapper(Class<T> type);
//
//	@Override
//	void close();
//
//	@Override
//	default void commit() {}
//
//	@Override
//	default void commit(boolean force) {}
//
//	@Override
//	default void rollback() {}
//
//	@Override
//	default void rollback(boolean force) {}
//
//	@Override
//	default Connection getConnection() {
//		throw new UnsupportedOperationException();
//	}
//}