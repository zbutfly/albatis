package net.butfly.albatis.bcp.utils;

import net.butfly.albacore.utils.IOs;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.bcp.Props;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.config.SocketConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.atomic.AtomicLong;

import static net.butfly.albatis.bcp.BcpFormat.logger;
import static net.butfly.albatis.bcp.Props.HTTP_RETRIES;


public class HttpUtils {
	private static CloseableHttpClient client = client();

	private static CloseableHttpClient client() {
		PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
		SocketConfig soconf = SocketConfig.custom()//
				.setSoKeepAlive(true)//
				.setSoReuseAddress(true)//
				.setTcpNoDelay(true)//
				.setSoTimeout(Props.HTTP_TIMEOUT)//
				.setRcvBufSize(Props.HTTP_RCV_BUF_SIZE).build();
		cm.setDefaultSocketConfig(soconf);
		cm.setMaxTotal(Props.HTTP_PARAL);
		cm.setDefaultMaxPerRoute(Props.HTTP_PARAL);
		RequestConfig reqconf = RequestConfig.custom()//
				.setConnectionRequestTimeout(Props.HTTP_TIMEOUT)//
				.setSocketTimeout(Props.HTTP_TIMEOUT)//
				.setConnectTimeout(Props.HTTP_TIMEOUT).build();
		return HttpClients.custom().setConnectionManager(cm).setDefaultRequestConfig(reqconf).build();
	}

	private static final Logger SLOW_LOG = Logger.getLogger("com.hzcominfo.dataggr.migrate.bcp.slow.http");
	private static final AtomicLong HTTP_CONC = new AtomicLong();
	static AtomicLong COUNT = new AtomicLong(0);
	static AtomicLong BYTES_HTTP = new AtomicLong(0);
	static AtomicLong MS_HTTP = new AtomicLong(0);

	static long last = System.currentTimeMillis();

	public static byte[] get(String url) {
		HttpGet get = new HttpGet(url);
		long conc = HTTP_CONC.incrementAndGet();
		try {
			long now = System.currentTimeMillis(), spent = 0;
			byte[] data = null;
			try {
				return data = IOs.readAll(client.execute(get).getEntity().getContent());
			} catch (UnsupportedOperationException | IOException e) {
				int n = 0;
				data = null;
				while (data == null && n++ < HTTP_RETRIES){
					HttpGet get2 = new HttpGet(url);
					try {
						logger.warn("[HTTP] retry "+n+" on [" + url + "]");
						data = IOs.readAll(client.execute(get2).getEntity().getContent());
					} catch (IOException ex) {
						data = null;
					}
				}
				logger.error("[HTTP] error on [" + url + "]: " + e.getMessage());
				return null;
			} finally {
				if ((spent = System.currentTimeMillis() - now) > 500) //
					SLOW_LOG.warn("[HTTP] slow [ms: " + spent + "], [conc: " + conc + "]: \n\t" + url);
				if (null != data && logger.isTraceEnabled()) {
					long ht = MS_HTTP.addAndGet(spent);
					long hb = BYTES_HTTP.addAndGet(data.length);
					long c = COUNT.incrementAndGet();
					if (c % Props.HTTP_STATS_STEP == 0) {
						double avg = Props.HTTP_STATS_STEP * 1000.0 / (System.currentTimeMillis() - last);
						avg = Math.round(avg * 100) / 100;
						logger.trace("[HTTP] summary: " + c + " recs, http avg: " + ht / c + " ms & " //
								+ hb / 1000.0 / c + " kb, current avg:" + avg + " get/s.");
						last = System.currentTimeMillis();
					}
				}
			}
		} finally {
			HTTP_CONC.decrementAndGet();
			get.releaseConnection();
		}
	}

	@Deprecated
	public static byte[] getWithJava(String httpUrl) throws IOException {
		HttpURLConnection conn = (HttpURLConnection) new URL(httpUrl).openConnection();
		try {
			conn.setRequestMethod("GET");
			conn.setConnectTimeout(Props.HTTP_TIMEOUT);
			conn.setReadTimeout(Props.HTTP_TIMEOUT);
			conn.setDoOutput(true);
			conn.setDoInput(true);
			conn.setRequestProperty("Content-type", "application/json,charset=UTF-8");
			try (InputStream is = conn.getInputStream();) {
				return IOs.readAll(is);
			}
		} finally {
			if (conn != null) conn.disconnect();
		}
	}

	public static void main(String[] args) throws IOException {
		long startTime = System.currentTimeMillis();
		byte[] result = get(
				"https://timgsa.baidu.com/timg?image&quality=80&size=b9999_10000&sec=1554186138023&di=609a102c76bfeb05e5605fd6d511a9a5&imgtype=0&src=http%3A%2F%2Fabco1.heibaimanhua.com%2Fwp-content%2Fuploads%2F2018%2F01%2F20180125_5a694e2bb9b8e.jpg");
		// FileUtils.generateImage(result, "E:\\lib\\2.jpg");
		long endTime = System.currentTimeMillis();
		System.out.println(endTime - startTime);
		System.out.println(result);
	}
}
