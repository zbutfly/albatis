package net.butfly.albatis.elastic;

import java.io.IOException;

import com.hzcominfo.albatis.nosql.AlbatisDriverManager;

import net.butfly.albacore.io.URISpec;

public class AlbatisDriver implements com.hzcominfo.albatis.nosql.AlbatisDriver {
	final static String schema = "es";
	static {
		try {
			AlbatisDriverManager.registerDriver(schema, new net.butfly.albatis.elastic.AlbatisDriver());
		} catch (Exception E) {
			throw new RuntimeException("Can't register driver!");
		}
	}

	public AlbatisDriver() throws Exception {
		// Required for Class.forName().newInstance()
	}

	@Override
	public com.hzcominfo.albatis.nosql.Connection connect(String url) {
		return connect(new URISpec(url));
	}
	
	@Override
	public com.hzcominfo.albatis.nosql.Connection connect(URISpec uriSpec) {
		try {
			return new ElasticConnection(uriSpec);
		} catch (IOException e) {
			throw new RuntimeException("Get " + uriSpec + " solr connection error!");
		}
	}
}
