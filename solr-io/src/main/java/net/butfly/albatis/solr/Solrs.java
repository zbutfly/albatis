package net.butfly.albatis.solr;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient.Builder;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.Pair;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import net.butfly.albacore.utils.Utils;
import net.butfly.albacore.utils.logger.Logger;

public final class Solrs extends Utils {
	protected static final Logger logger = Logger.getLogger(Solrs.class);
	private static final Map<Class<? extends ResponseParser>, ResponseParser> parsers = new ConcurrentHashMap<>();
	static {
		XMLResponseParser d = new XMLResponseParser();
		parsers.put(d.getClass(), d);
	}
	private static final Map<String, SolrClient> clients = new ConcurrentHashMap<>();
	private static final HttpClient DEFAULT_HTTP_CLIENT;
	static {
		ModifiableSolrParams params = new ModifiableSolrParams();
		params.set(HttpClientUtil.PROP_MAX_CONNECTIONS, 4096);
		params.set(HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, 1024);
		// not allow redirects
		params.set(HttpClientUtil.PROP_FOLLOW_REDIRECTS, false);
		params.set(HttpClientUtil.PROP_CONNECTION_TIMEOUT, 10000);
		params.set(HttpClientUtil.PROP_SO_TIMEOUT, 10000);

		DEFAULT_HTTP_CLIENT = HttpClientUtil.createClient(params);
	}

	private Solrs() {}

	public static SolrClient open(String solrURL, Class<? extends ResponseParser> parserClass) {
		logger.debug("Solr open: " + solrURL);
		return clients.computeIfAbsent(solrURL, url -> client(url, parserClass));
	}

	private static SolrClient client(String url, Class<? extends ResponseParser> parserClass) {
		URI uri;
		try {
			uri = new URI(url);
		} catch (URISyntaxException e) {
			throw new RuntimeException(e);
		}
		switch (uri.getScheme().toLowerCase()) {
		case "http":
			Builder hb = new HttpSolrClient.Builder(url).allowCompression(true).withHttpClient(DEFAULT_HTTP_CLIENT);
			if (null != parserClass) hb = hb.withResponseParser(parsers.computeIfAbsent(parserClass, clz -> parser(clz)));
			logger.info("Solr client create: " + url);
			return hb.build();
		case "zookeeper":
			CloudSolrClient.Builder cb = new CloudSolrClient.Builder();
			logger.info("Solr client create by zookeeper: " + uri.getAuthority());
			CloudSolrClient c = cb.withZkHost(Arrays.asList(uri.getAuthority().split(","))).withHttpClient(DEFAULT_HTTP_CLIENT).build();
			c.setZkClientTimeout(Integer.parseInt(System.getProperty("albatis.io.zkclient.timeout", "15000")));
			c.setZkConnectTimeout(Integer.parseInt(System.getProperty("albatis.io.zkconnect.timeout", "15000")));
			c.setParallelUpdates(true);
			return c;
		default:
			throw new RuntimeException("Solr open failure, invalid url: " + url);
		}
	}

	private static ResponseParser parser(Class<? extends ResponseParser> clz) {
		try {
			return clz.newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

	public static SolrClient open(String solrURL) {
		return open(solrURL, null);
	}

	public static SolrDocumentList query(String url, SolrQuery query) throws SolrServerException, IOException {
		return open(url).query(query).getResults();
	}

	public static QueryResponse queryForResponse(String url, SolrQuery query) throws SolrServerException, IOException {
		return open(url).query(query);
	}

	public static QueryResponse[] query(SolrQuery query, String... url) {
		if (null == url || null == query) return null;
		if (url.length == 0) return new QueryResponse[0];
		if (url.length == 1) try {
			return new QueryResponse[] { queryForResponse(url[0], query) };
		} catch (SolrServerException | IOException e) {
			logger.error("Solr concurrent query failure", e);
			return new QueryResponse[0];
		}
		ListeningExecutorService ex = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
		List<ListenableFuture<QueryResponse>> fs = new ArrayList<ListenableFuture<QueryResponse>>();
		for (String u : url)
			fs.add(ex.submit(new Callable<QueryResponse>() {
				@Override
				public QueryResponse call() {
					if (null == u) return null;
					try {
						return open(u).query(query);
					} catch (Exception e) {
						logger.error("Solr concurrent query failure", e);
						return null;
					}
				}
			}));
		ListenableFuture<List<QueryResponse>> ff = Futures.successfulAsList(fs);
		try {
			return ff.get().toArray(new QueryResponse[0]);
		} catch (InterruptedException e) {
			return new QueryResponse[0];
		} catch (ExecutionException e) {
			logger.error("Solr concurrent query failure", e.getCause());
			return new QueryResponse[0];
		} finally {
			ex.shutdown();
		}
	}

	public static QueryResponse[] query(String url, SolrQuery... query) {
		if (null == url || null == query) return null;
		if (query.length == 0) return new QueryResponse[0];
		if (query.length == 1) try {
			return new QueryResponse[] { queryForResponse(url, query[0]) };
		} catch (SolrServerException | IOException e) {
			logger.error("Solr concurrent query failure", e);
			return new QueryResponse[0];
		}
		ListeningExecutorService ex = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
		List<ListenableFuture<QueryResponse>> fs = new ArrayList<ListenableFuture<QueryResponse>>();
		for (SolrQuery q : query)
			fs.add(ex.submit(new Callable<QueryResponse>() {
				@Override
				public QueryResponse call() {
					if (null == q) return null;
					try {
						return open(url).query(q);
					} catch (Exception e) {
						logger.error("Solr concurrent query failure", e);
						return null;
					}
				}
			}));
		ListenableFuture<List<QueryResponse>> ff = Futures.successfulAsList(fs);
		try {
			return ff.get().toArray(new QueryResponse[0]);
		} catch (InterruptedException e) {
			return new QueryResponse[0];
		} catch (ExecutionException e) {
			logger.error("Solr concurrent query failure", e.getCause());
			return new QueryResponse[0];
		} finally {
			ex.shutdown();
		}
	}

	@SafeVarargs
	public static QueryResponse[] query(Pair<String, SolrQuery>... query) {
		if (null == query) return null;
		if (query.length == 0) return new QueryResponse[0];
		if (query.length == 1) try {
			return new QueryResponse[] { queryForResponse(query[0].first(), query[0].second()) };
		} catch (SolrServerException | IOException e) {
			logger.error("Solr concurrent query failure", e);
			return new QueryResponse[0];
		}
		ListeningExecutorService ex = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
		List<ListenableFuture<QueryResponse>> fs = new ArrayList<ListenableFuture<QueryResponse>>();
		for (Pair<String, SolrQuery> q : query)
			fs.add(ex.submit(new Callable<QueryResponse>() {
				@Override
				public QueryResponse call() {
					if (null == q) return null;
					try {
						return open(q.first()).query(q.second());
					} catch (Exception e) {
						logger.error("Solr concurrent query failure", e);
						return null;
					}
				}
			}));
		ListenableFuture<List<QueryResponse>> ff = Futures.successfulAsList(fs);
		try {
			return ff.get().toArray(new QueryResponse[0]);
		} catch (InterruptedException e) {
			return new QueryResponse[0];
		} catch (ExecutionException e) {
			logger.error("Solr concurrent query failure", e.getCause());
			return new QueryResponse[0];
		} finally {
			ex.shutdown();
		}
	}

	public static QueryResponse[] query(List<Pair<String, SolrQuery>> query) {
		if (null == query) return null;
		if (query.size() == 0) return new QueryResponse[0];
		if (query.size() == 1) try {
			return new QueryResponse[] { queryForResponse(query.get(0).first(), query.get(0).second()) };
		} catch (SolrServerException | IOException e) {
			logger.error("Solr concurrent query failure", e);
			return new QueryResponse[0];
		}
		ListeningExecutorService ex = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
		List<ListenableFuture<QueryResponse>> fs = new ArrayList<ListenableFuture<QueryResponse>>();
		for (Pair<String, SolrQuery> q : query)
			fs.add(ex.submit(new Callable<QueryResponse>() {
				@Override
				public QueryResponse call() {
					if (null == q) return null;
					try {
						return open(q.first()).query(q.second());
					} catch (Exception e) {
						logger.error("Solr concurrent query failure", e);
						return null;
					}
				}
			}));
		ListenableFuture<List<QueryResponse>> ff = Futures.successfulAsList(fs);
		try {
			return ff.get().toArray(new QueryResponse[0]);
		} catch (InterruptedException e) {
			return new QueryResponse[0];
		} catch (ExecutionException e) {
			logger.error("Solr concurrent query failure", e.getCause());
			return new QueryResponse[0];
		} finally {
			ex.shutdown();
		}
	}
}
