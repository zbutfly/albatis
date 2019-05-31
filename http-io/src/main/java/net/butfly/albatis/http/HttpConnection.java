package net.butfly.albatis.http;

import java.io.IOException;

import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.HttpClientBuilder;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.DataConnection;

public class HttpConnection extends DataConnection<HttpClient> {
	public HttpClient httpClient;

	public HttpConnection(URISpec uri) throws IOException {
		super(uri);
	}

	public HttpConnection(String urispec) throws IOException {
		this(new URISpec(urispec));
	}

	protected HttpClient initialize(URISpec uri) {

		RequestConfig defaultRequestConfig = RequestConfig.custom().setConnectTimeout(30000)
				.setConnectionRequestTimeout(30000).setSocketTimeout(30000).build();
		httpClient = (HttpClient) HttpClientBuilder.create().setDefaultRequestConfig(defaultRequestConfig);

		return httpClient;
	}

	public HttpInput inputRaw() {
		return new HttpInput("HttpInput", httpClient);
	}
}
