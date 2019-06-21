package net.butfly.albatis.http;

import java.io.IOException;
import java.util.List;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.DataConnection;
import net.butfly.albatis.ddl.TableDesc;

public class HttpConnection extends DataConnection<HttpClient> {
	final static String schema = "http";
	public HttpClient httpClient;
	public URISpec url;
	
	public HttpConnection(URISpec uri) throws IOException {
		super(uri, schema);
		url =uri;
	}

	public HttpConnection(String urispec) throws IOException {
		this(new URISpec(urispec));
	}

	protected HttpClient initialize(URISpec url) {
		httpClient = new DefaultHttpClient();
		return httpClient;
	}

	public HttpInput inputRaw(TableDesc... table) {
		return new HttpInput("HttpInput", this);
	}

	public static class Driver implements net.butfly.albatis.Connection.Driver<HttpConnection> {
		static {
			DriverManager.register(new Driver());
		}

		@Override
		public HttpConnection connect(URISpec uriSpec) throws IOException {
			return new HttpConnection(uriSpec);
		}

		@Override
		public List<String> schemas() {
			return Colls.list("http");
		}
	}

	public List<String> schemas() {
		return Colls.list("http");
	}

	@Override
	public void close() {
		try {
			super.close();
		} catch (IOException e) {
			logger.error("Close failure", e);
		}
	}
}
