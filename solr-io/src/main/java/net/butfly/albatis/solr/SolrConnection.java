package net.butfly.albatis.solr;

import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

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
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient.Builder;
import org.apache.solr.client.solrj.impl.HttpSolrClient.RemoteSolrException;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.DelegationTokenResponse.JsonMapResponseParser;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.params.ModifiableSolrParams;

import com.hzcominfo.albatis.nosql.Connection;
import com.hzcominfo.albatis.nosql.NoSqlConnection;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Message;

public class SolrConnection extends NoSqlConnection<SolrClient> {
	private static final Logger logger = Logger.getLogger(SolrConnection.class);
	private static final CloseableHttpClient HTTP_CLIENT = SolrHttpContext.createDefaultHttpclient();
	private static final Map<Class<? extends ResponseParser>, ResponseParser> PARSER_POOL = Maps.of();
	private final SolrMeta meta;

	public enum ResponseFormat {
		XML(XMLResponseParser.class), JSON(JsonMapResponseParser.class), BINARY(BinaryResponseParser.class);
		private final Class<? extends ResponseParser> parser;

		private <P extends ResponseParser> ResponseFormat(Class<P> parser) {
			this.parser = parser;
		}

		@SuppressWarnings("unchecked")
		private static Class<? extends ResponseParser> parse(String id) {
			try {
				return ResponseFormat.valueOf(id.toUpperCase()).parser;
			} catch (Exception e) {
				try {
					return (Class<? extends ResponseParser>) Class.forName(id);
				} catch (ClassNotFoundException e1) {
					throw new IllegalArgumentException("Parser [" + id + "] not found.");
				}
			}
		}
	}

	public SolrConnection(String connection) throws IOException {
		this(new URISpec(connection));
	}

	public SolrConnection(String connection, Class<? extends ResponseParser> parserClass) throws IOException {
		this(new URISpec(connection), parserClass);
	}

	public SolrConnection(URISpec uri) throws IOException {
		this(uri.getScheme().startsWith("solr:http") ? new URISpec(uri.toString().replace("solr:", "")) : uri, ResponseFormat.parse(uri
				.getParameter("parser", "XML")));
	}

	public SolrConnection(URISpec uri, Class<? extends ResponseParser> parserClass) throws IOException {
		this(uri, parserClass, true);
	}

	public SolrConnection(URISpec uri, boolean parsing) throws IOException {
		this(uri, ResponseFormat.parse(uri.getParameter("parser", "XML")), parsing);
	}

	public SolrConnection(URISpec uri, Class<? extends ResponseParser> parserClass, boolean parsing) throws IOException {
		super(uri, u -> create(u, parserClass), "solr", "zk:solr", "zookeeper", "zk", "http");
		meta = parsing ? SolrMeta.parse(uri) : null;
	}

	private static SolrClient create(URISpec uri, Class<? extends ResponseParser> parserClass) {
		if (null == parserClass)
			parserClass = XMLResponseParser.class;
		ResponseParser p = PARSER_POOL.computeIfAbsent(parserClass, clz -> Reflections.construct(clz));
		switch (uri.getScheme()) {
		case "solr:http":
		case "http":
			logger.debug("Solr client create: " + uri);
			Builder hb = new HttpSolrClient.Builder(uri.toString().replaceAll("/$", "")).allowCompression(true).withHttpClient(HTTP_CLIENT)//
					.withResponseParser(p);
			return hb.build();
		case "solr":
		case "solr:zk":
		case "zookeeper":
		case "zk":
		case "zk:solr":
			logger.debug("Solr client create by zookeeper: " + uri);
			CloudSolrClient.Builder cb = new CloudSolrClient.Builder();
			CloudSolrClient c = cb.withZkHost(Arrays.asList(uri.getHost().split(","))).withHttpClient(HTTP_CLIENT).build();
			c.setZkClientTimeout(Integer.parseInt(System.getProperty("albatis.io.zkclient.timeout", "30000")));
			c.setZkConnectTimeout(Integer.parseInt(System.getProperty("albatis.io.zkconnect.timeout", "30000")));
			c.setParallelUpdates(true);
			c.setParser(p);
			return c;
		default:
			throw new RuntimeException("Solr open failure, invalid uri: " + uri);
		}
	}

	public String getDefaultCore() {
		return meta.defCore;
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
			HttpClientUtil.addRequestInterceptor((req, ctx) -> {
			});
			return HttpClientUtil.createClient(params);
		}

		static CloseableHttpClient createDefaultHttpclient() {
			// return
			// HttpClients.custom().setDefaultRequestConfig(HTTP_REQ_CONFIG).setMaxConnPerRoute(HTTP_MAX_PER_ROUTE).setMaxConnTotal(HTTP_MAX_TOTAL).build();
			PoolingHttpClientConnectionManager m = new PoolingHttpClientConnectionManager();
			m.setMaxTotal(SolrHttpContext.SOLR_HTTP_MAX_TOTAL);
			m.setDefaultMaxPerRoute(SolrHttpContext.SOLR_HTTP_MAX_PER_ROUTE);
			m.setDefaultSocketConfig(SocketConfig.custom().setSoTimeout(SolrHttpContext.SOLR_HTTP_SO_TIMEOUT).build());
			m.setDefaultConnectionConfig(
					ConnectionConfig.custom().setBufferSize(SolrHttpContext.SOLR_HTTP_BUFFER_SIZE).build());
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
		client().close();
	}

	public String[] getCores() {
		return meta.allCores;
	}

	public String getBase() {
		return meta.baseUrl;
	}

	public static class SolrMeta implements Serializable {
		private static final long serialVersionUID = 306397699133380778L;
		final String baseUrl;
		final String defCore;
		final String[] allCores;

		public SolrMeta(String baseUrl, String defCore, String... allCores) {
			super();
			this.baseUrl = baseUrl;
			this.defCore = defCore;
			this.allCores = allCores;
		}

		@Override
		public String toString() {
			return "SolrMeta: " + baseUrl + " [default core: " + defCore + "]" + (allCores.length == 0 ? ""
					: ", [all cores: " + String.join(",", allCores) + "]");
		}

		/**
		 * @param uri
		 * @return Tuple3: <baseURL, defaultCore[maybe null], allCores>, or null for invalid uri
		 * @throws IOException
		 * @throws SolrServerException
		 * @throws URISyntaxException
		 */
		public static SolrMeta parse(URISpec uri) throws IOException {
			try {
				return parse0(uri.resolve("."), uri.getFile());
			} catch (IOException e) {
				if (null != uri.getFile()) try {
					return parse0(new URISpec(uri.toString() + "/"), null);
				} catch (IOException ee) {}
				URISpec uri1 = uri.resolve("..").setFile(uri.getFile());
				if (!uri1.equals(uri)) return parse(uri1);
				throw new IOException("Solr uri base parsing failure: " + uri);
			}
		}

		private static SolrMeta parse0(URISpec base, String file) throws IOException {
			CoreAdminRequest req = new CoreAdminRequest();
			req.setAction(CoreAdminAction.STATUS);
			try (SolrConnection solr = new SolrConnection(base, false);) {
				CoreAdminResponse resp = req.process(solr.client());
				String[] cores = new String[resp.getCoreStatus().size()];
				for (int i = 0; i < resp.getCoreStatus().size(); i++)
					cores[i] = resp.getCoreStatus().getName(i);
				return new SolrMeta(base.toString().replaceAll("/$", ""), file, cores);
			} catch (RemoteSolrException | SolrServerException e) {
				throw new IOException(e);
			}
		}
	}

	public enum ResponseFormat {
		XML(XMLResponseParser.class), JSON(JsonMapResponseParser.class), BINARY(BinaryResponseParser.class);
		private final Class<? extends ResponseParser> parser;

		private <P extends ResponseParser> ResponseFormat(Class<P> parser) {
			this.parser = parser;
		}

		@SuppressWarnings("unchecked")
		private static Class<? extends ResponseParser> parse(String id) {
			try {
				return ResponseFormat.valueOf(id.toUpperCase()).parser;
			} catch (Exception e) {
				try {
					return (Class<? extends ResponseParser>) Class.forName(id);
				} catch (ClassNotFoundException e1) {
					throw new IllegalArgumentException("Parser [" + id + "] not found.");
				}
			}
		}
	}

	public static class Driver implements com.hzcominfo.albatis.nosql.Connection.Driver<SolrConnection> {
		static {
			DriverManager.register(new Driver());
		}

		@Override
		public SolrConnection connect(URISpec uriSpec) throws IOException {
			return new SolrConnection(uriSpec);
		}

		@Override
		public List<String> schemas() {
			return Colls.list("solr", "solr:zk");
		}
	}

	@Override
	public Input<Message> input(String... table) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public SolrOutput output() throws IOException {
		return new SolrOutput("SolrOutput", this);
	}

	public static void main(String... args) throws IOException {
		testParse("http://data03:10180/solr/zhk_SJZYZH_MY_TRAINFO");
		testParse("http://data03:10180/solr/");
		testParse("http://data03:10180/solr");
	}

	private static void testParse(String u) throws IOException {
		System.out.println("Parse Solr URI [" + u + "] => " + SolrMeta.parse(new URISpec(u)));
	}
}
