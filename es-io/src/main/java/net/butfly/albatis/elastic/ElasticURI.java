package net.butfly.albatis.elastic;

import net.butfly.albacore.io.URISpec;

/**
 * Created by hzcominfo67 on 2017/1/19.
 */
public class ElasticURI extends URISpec {
	public ElasticURI(String str) {
		super(str);
	}

	public ElasticURI(String str, int defaultPort) {
		super(str, defaultPort);
	}
}
