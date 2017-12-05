package com.hzcominfo.albatis.nosql;

import net.butfly.albacore.io.URISpec;

public interface AlbatisDriver {

	public com.hzcominfo.albatis.nosql.Connection connect(String url);

	public com.hzcominfo.albatis.nosql.Connection connect(URISpec uriSpec);
}
