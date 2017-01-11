/*
 * 文件名：Connection.java
 * 版权：
 * 描述：数据库链接接口
 * 创建人： 郎敬翔
 * 修改时间：2016-11-24
 * 操作：创建
 */
package com.hzcominfo.albatis.search.driver;

import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.util.Properties;

import com.hzcominfo.albatis.search.Action;
import com.hzcominfo.albatis.search.Describe;
import com.hzcominfo.albatis.search.Query;
import com.hzcominfo.albatis.search.exception.SearchAPIException;
import com.hzcominfo.albatis.search.result.Result;

/**
 * Created by ljx on 2016/11/24.
 *
 * @author ljx
 * @date 2016-11-24
 */
public interface Connection extends java.sql.Connection, com.hzcominfo.albatis.nosql.Connection {

	Query getQuery();

	@Override
	void close() throws SQLException;

	void obtainDrive(DriverType type) throws SearchAPIException;

	void setUri(URI uri);

	void setProperties(Properties properties);

	Result execute(Action action, Describe describe) throws SearchAPIException, IOException;
}