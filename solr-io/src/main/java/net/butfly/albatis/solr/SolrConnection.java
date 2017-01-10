package net.butfly.albatis.solr;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.http.HttpRequestInterceptor;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.SocketConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient.Builder;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;

import com.hzcominfo.albatis.nosql.NoSqlConnection;

import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.logger.Logger;

public class SolrConnection extends NoSqlConnection<SolrClient> {
	private static final Logger logger = Logger.getLogger(SolrConnection.class);
	private static final CloseableHttpClient HTTP_CLIENT = SolrHttpContext.createDefaultHttpclient();
	private static final Map<Class<? extends ResponseParser>, ResponseParser> PARSER_POOL = new ConcurrentHashMap<>();

	private String baseUri;
	private String defaultCore;
	private String[] cores;
	private final Class<? extends ResponseParser> parserClass;

	public SolrConnection(String connection) throws IOException {
		this(connection, XMLResponseParser.class);
	}

	public <P extends ResponseParser> SolrConnection(String connection, Class<P> parserClass) throws IOException {
		super(connection, "solr", "zookeeper", "http");
		this.parserClass = parserClass;
		parse();

	}

	public String getDefaultCore() {
		return defaultCore;
	}

	@Override
	protected SolrClient createClient(URI uri) throws IOException {
		ResponseParser p = PARSER_POOL.computeIfAbsent(parserClass, clz -> Reflections.construct(clz));
		switch (supportedSchema(uri.getScheme())) {
		case "http":
			logger.debug("Solr client create: " + uri);
			Builder hb = new HttpSolrClient.Builder(uri.toString()).allowCompression(true).withHttpClient(HTTP_CLIENT)//
					.withResponseParser(p);
			return hb.build();
		case "solr":
		case "zookeeper":
			logger.debug("Solr client create by zookeeper: " + uri.getAuthority());
			CloudSolrClient.Builder cb = new CloudSolrClient.Builder();
			CloudSolrClient c = cb.withZkHost(Arrays.asList(uri.getAuthority().split(","))).withHttpClient(HTTP_CLIENT).build();
			c.setZkClientTimeout(Integer.parseInt(System.getProperty("albatis.io.zkclient.timeout", "30000")));
			c.setZkConnectTimeout(Integer.parseInt(System.getProperty("albatis.io.zkconnect.timeout", "30000")));
			c.setParallelUpdates(true);
			c.setParser(p);
			return c;
		default:
			throw new RuntimeException("Solr open failure, invalid uri: " + uri);
		}
	}

	/**
	 * @param uri
	 * @return Tuple3: <baseURL, defaultCore[maybe null], allCores>, or null for
	 *         invalid uri
	 * @throws IOException
	 * @throws SolrServerException
	 * @throws URISyntaxException
	 */
	private void parse() throws IOException {
		String url = uri.toString();
		CoreAdminRequest req = new CoreAdminRequest();
		req.setAction(CoreAdminAction.STATUS);
		try {
			CoreAdminResponse resp = req.process(getClient());
			cores = new String[resp.getCoreStatus().size()];
			for (int i = 0; i < resp.getCoreStatus().size(); i++)
				getCores()[i] = resp.getCoreStatus().getName(i);
			baseUri = url;
			defaultCore = null;
		} catch (SolrServerException e) { // XXX IOException?
			String path;
			try {
				path = new URI(url).getPath();
			} catch (URISyntaxException e1) {
				throw new IOException("Solr uri invalid: " + url);
			}
			if (null == path) throw new IOException("Solr uri invalid: " + uri);
			List<String> segs = new ArrayList<>(Arrays.asList(path.split("/+")));
			if (segs.isEmpty()) throw new IOException("Solr uri invalid: " + uri);
			defaultCore = segs.remove(segs.size() - 1);
			baseUri = url.replaceAll("/?" + defaultCore + "/?$", "");
			try (SolrConnection conn = new SolrConnection(baseUri, parserClass)) {
				cores = conn.getCores();
			}
		}
	}

	interface SolrHttpContext {
		static final int SOLR_HTTP_MAX_TOTAL = 1024;
		static final int SOLR_HTTP_MAX_PER_ROUTE = 64;
		static final int SOLR_HTTP_SO_TIMEOUT = 180000;
		static final int SOLR_HTTP_CONN_TIMEOUT = 60000;
		static final int SOLR_HTTP_BUFFER_SIZE = 102400;
		static final RequestConfig SOLR_HTTP_REQ_CONFIG = RequestConfig.custom()//
				.setSocketTimeout(SOLR_HTTP_SO_TIMEOUT)//
				.setConnectTimeout(SOLR_HTTP_CONN_TIMEOUT)//
				.setRedirectsEnabled(false)//
				.build();
		static final HttpRequestInterceptor SOLR_HTTP_ICPT = (req, ctx) -> {
			req.setHeader("Connection", "Keep-Alive");
			logger.trace("Solr sent: " + req.toString());
		};

		static CloseableHttpClient createDefaultHttpclientBySolr() {
			ModifiableSolrParams params = new ModifiableSolrParams();
			params.set(HttpClientUtil.PROP_MAX_CONNECTIONS, SolrHttpContext.SOLR_HTTP_MAX_TOTAL);
			params.set(HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, SolrHttpContext.SOLR_HTTP_MAX_PER_ROUTE);
			// not allow redirects
			params.set(HttpClientUtil.PROP_FOLLOW_REDIRECTS, false);
			params.set(HttpClientUtil.PROP_CONNECTION_TIMEOUT, SolrHttpContext.SOLR_HTTP_CONN_TIMEOUT);
			params.set(HttpClientUtil.PROP_SO_TIMEOUT, SolrHttpContext.SOLR_HTTP_SO_TIMEOUT);
			HttpClientUtil.addRequestInterceptor((req, ctx) -> {});
			return HttpClientUtil.createClient(params);
		}

		static CloseableHttpClient createDefaultHttpclient() {
			// return
			// HttpClients.custom().setDefaultRequestConfig(HTTP_REQ_CONFIG).setMaxConnPerRoute(HTTP_MAX_PER_ROUTE).setMaxConnTotal(HTTP_MAX_TOTAL).build();
			PoolingHttpClientConnectionManager m = new PoolingHttpClientConnectionManager();
			m.setMaxTotal(SolrHttpContext.SOLR_HTTP_MAX_TOTAL);
			m.setDefaultMaxPerRoute(SolrHttpContext.SOLR_HTTP_MAX_PER_ROUTE);
			m.setDefaultSocketConfig(SocketConfig.custom().setSoTimeout(SolrHttpContext.SOLR_HTTP_SO_TIMEOUT).build());
			m.setDefaultConnectionConfig(ConnectionConfig.custom().setBufferSize(SolrHttpContext.SOLR_HTTP_BUFFER_SIZE).build());
			return HttpClientBuilder.create()//
					.addInterceptorLast(SolrHttpContext.SOLR_HTTP_ICPT)//
					.setConnectionManager(m)//
					.setDefaultRequestConfig(SolrHttpContext.SOLR_HTTP_REQ_CONFIG)//
					.build();
		}

		static CloseableHttpAsyncClient createDefaultAsyncHttpclient() {
			return HttpAsyncClients.custom()//
					.addInterceptorLast(SolrHttpContext.SOLR_HTTP_ICPT)//
					.setDefaultRequestConfig(SolrHttpContext.SOLR_HTTP_REQ_CONFIG)//
					.setMaxConnPerRoute(SolrHttpContext.SOLR_HTTP_MAX_PER_ROUTE)//
					.setMaxConnTotal(SolrHttpContext.SOLR_HTTP_MAX_TOTAL)//
					.build();
		}
	}

	@Override
	public void close() throws IOException {
		super.close();
		getClient().close();
	}

	public String[] getCores() {
		return cores;
	}
}
