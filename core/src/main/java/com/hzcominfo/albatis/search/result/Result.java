/*
 * 文件名：result.java
 * 版权： 
 * 描述:所有结果集的继承类
 * 创建人： 郎敬翔
 * 修改时间：2016-11-14
 * 操作：创建
 */
package com.hzcominfo.albatis.search.result;

import java.sql.ResultSet;
import java.util.Map;

import com.hzcominfo.albatis.search.exception.SearchAPIException;

/**
 * 描述返回结果的数据表，参考JDBC的ResultSet设定，其中也包括了一部分对Nosql数据特定格式的处理<br>
 * 接口提供的方法对于不同的数据库，不一定全部实现，对于未实现的方法，除了利用default标记以外，建议实现描述更加清晰的异常输出
 *
 * @author lic
 * @version 0.0.1
 * @see java.sql.ResultSet
 * @since 0.0.1
 */
public interface Result extends ResultSet {
	@SuppressWarnings("rawtypes")
	default Map getRowItem() throws SearchAPIException {
		throw new UnsupportedOperationException();
	}

	default ResultMetadata getResultMetadata() throws SearchAPIException {
		throw new UnsupportedOperationException();
	}

	default void setResultMetadata(ResultMetadata resultMetadata) throws SearchAPIException {
		throw new UnsupportedOperationException();
	}
}
