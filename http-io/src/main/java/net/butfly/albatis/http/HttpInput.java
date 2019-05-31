package net.butfly.albatis.http;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.util.EntityUtils;

import com.fasterxml.jackson.databind.ObjectMapper;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.io.OddInput;
import net.butfly.albatis.io.Rmap;

public class HttpInput extends net.butfly.albacore.base.Namedly implements OddInput<Rmap> {
	private static final long serialVersionUID = -7766621067006571958L;
	public String table;
	public URISpec url;
	private static HttpClient client;

	public HttpInput(String name, HttpClient client) {
		super(name);
		this.client = client;
	}

	public static ThreadLocal<ObjectMapper> httpGetMapper = new ThreadLocal<ObjectMapper>() {
		@Override
		protected ObjectMapper initialValue() {
			return new ObjectMapper();
		}
	};
	public static ThreadLocal<ObjectMapper> httpPostMapper = new ThreadLocal<ObjectMapper>() {
		@Override
		protected ObjectMapper initialValue() {
			return new ObjectMapper();
		}
	};

	private static String httpPost(URISpec uri) {
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
		String now = df.format(new Date());
		String Url = "http://" + uri.getHost() + ":" + uri.getDefaultPort() + uri.getUsername() + "?"
				+ uri.getPassword() + now;
		HttpPost httppost = new HttpPost(Url);
		HttpResponse response = null;
		try {
			response = client.execute(httppost, HttpClientContext.create());
			HttpEntity entity = response.getEntity();
			String result = EntityUtils.toString(entity, "utf-8");
			return result;
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	@SuppressWarnings("deprecation")
	@Override
	public Rmap dequeue() {
		while (opened()) {
			String m = httpPost(url);
			return new Rmap(table, m);
		}
		return null;
	}
}
