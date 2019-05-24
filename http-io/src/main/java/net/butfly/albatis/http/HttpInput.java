package net.butfly.albatis.http;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.io.OddInput;
import net.butfly.albatis.io.Rmap;

public class HttpInput extends net.butfly.albacore.base.Namedly implements OddInput<Rmap> {
	private static final long serialVersionUID = -7766621067006571958L;
	public String table;
	public URISpec url;
	public String param;

	@SuppressWarnings("deprecation")
	public HttpInput(String table, URISpec url, String... param) {
		super();
		this.name = table;
	}

	private String httpPost(String table, URISpec uri, String param) {
		String Url = url + "?manufactorID=12332&sendTime=" + param + "&dataType=1&token=xdjfjjdjdjdd43333";
		PrintWriter out = null;
		InputStream is = null;
		BufferedReader br = null;
		String result = "";
		HttpURLConnection conn = null;
		StringBuffer strBuffer = new StringBuffer();
		try {
			URL realUrl = new URL(Url);
			conn = (HttpURLConnection) realUrl.openConnection();
			conn.setRequestMethod("POST");
			conn.setConnectTimeout(20000);
			conn.setReadTimeout(300000);
			conn.setRequestProperty("Charset", "UTF-8");
			conn.setRequestProperty("Content-Type", "application/json");
			conn.setRequestProperty("Content-Encoding", "utf-8");
			conn.setDoOutput(true);
			conn.setDoInput(true);
			conn.setUseCaches(false);
			out = new PrintWriter(conn.getOutputStream());
			out.print(param);
			out.flush();
			is = conn.getInputStream();
			br = new BufferedReader(new InputStreamReader(is));
			String line = null;
			while ((line = br.readLine()) != null) {
				strBuffer.append(line);
			}
			result = strBuffer.toString();
		} catch (Exception e) {
			System.out.println("发送 POST 请求出现异常！" + e);
			e.printStackTrace();
		} finally {
			try {
				if (out != null) {
					out.close();
				}
				if (br != null) {
					br.close();
				}
				if (conn != null) {
					conn.disconnect();
				}
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		return result;
	}

	@SuppressWarnings("deprecation")
	@Override
	public Rmap dequeue() {
		while (opened()) {
			String m = httpPost(table, url, param);
			return new Rmap(table, m);
		}
		return null;
	}
}
