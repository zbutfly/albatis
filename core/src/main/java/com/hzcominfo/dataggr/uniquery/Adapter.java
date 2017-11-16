package com.hzcominfo.dataggr.uniquery;

import org.apache.calcite.sql.SqlNode;

import com.hzcominfo.albatis.nosql.Connection;

import net.butfly.albacore.io.URISpec;

public interface Adapter {

	public static Adapter adapt(URISpec uriSpec) {
		return null;
	}
	
	// 查询组装
    public <T> T queryAssemble(SqlNode sqlNode, Object...params);
    
    // 查询执行
    public <T> T queryExecute(Connection connection, Object query);
    
    // 结果组装
    public <T> T resultAssemble(Object result);
}
