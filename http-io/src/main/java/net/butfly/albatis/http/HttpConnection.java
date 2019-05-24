package net.butfly.albatis.http;

import java.io.IOException;

import org.apache.http.client.HttpClient;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.DataConnection;

public class HttpConnection extends DataConnection<HttpClient>{

	public HttpConnection(URISpec uri) throws IOException{
		this(uri,"http");
	}
	
	public HttpConnection(URISpec uri, String... param) throws IOException {
		super(uri,param);
	}

	public HttpInput input(String... tables) {
		return new HttpInput("HttpInut", uri, tables);		
	}
}
