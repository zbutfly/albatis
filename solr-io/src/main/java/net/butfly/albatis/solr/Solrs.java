package net.butfly.albatis.solr;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient.RemoteSolrException;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.util.Pair;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import net.butfly.albacore.utils.Utils;
import net.butfly.albacore.utils.logger.Logger;
import scala.Tuple3;

@Deprecated
public final class Solrs extends Utils {
	protected static final Logger logger = Logger.getLogger(Solrs.class);
	private static final Map<String, SolrClient> clients = new ConcurrentHashMap<>();

	private Solrs() {}

	@SuppressWarnings("resource")
	public static SolrClient open(String solrURL, Class<? extends ResponseParser> parserClass) {
		try {
			return new SolrConnection(solrURL, parserClass).client();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static void close(SolrClient solr) {
		List<String> keys = new ArrayList<>();
		for (Entry<String, SolrClient> e : clients.entrySet())
			if (e.getValue().equals(solr)) {
				logger.debug("Solr close: " + e.getKey());
				keys.add(e.getKey());
			}
		if (keys.isEmpty()) {
			CoreAdminRequest req = new CoreAdminRequest();
			req.setAction(CoreAdminAction.STATUS);
			try {
				logger.debug("Solr close: not registered client with uri [" + req.process(solr).getRequestUrl() + "]");
			} catch (SolrServerException | IOException e) {
				logger.debug("Solr close: not registered client with unknown uri.");
			}
		} else for (String k : keys)
			clients.remove(k);
		try {
			solr.close();
		} catch (IOException e) {
			logger.error("Solr close failure", e);
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

	/**
	 * @param uri
	 * @return Tuple3: <baseURL, defaultCore[maybe null], allCores>, or null for
	 *         invalid uri
	 * @throws IOException
	 * @throws SolrServerException
	 * @throws URISyntaxException
	 */
	public static Tuple3<String, String, String[]> parseSolrURL(String url) throws IOException, SolrServerException, URISyntaxException {
		url = new URI(url).toASCIIString();
		CoreAdminRequest req = new CoreAdminRequest();
		req.setAction(CoreAdminAction.STATUS);
		try {
			CoreAdminResponse resp = req.process(Solrs.open(url));
			String[] cores = new String[resp.getCoreStatus().size()];
			for (int i = 0; i < resp.getCoreStatus().size(); i++)
				cores[i] = resp.getCoreStatus().getName(i);
			return new Tuple3<>(url, null, cores);
		} catch (RemoteSolrException e) {
			String path;
			try {
				path = new URI(url).getPath();
			} catch (URISyntaxException e1) {
				throw new SolrServerException("Solr uri invalid: " + url);
			}
			if (null == path) throw new SolrServerException("Solr uri invalid: " + url);
			List<String> segs = new ArrayList<>(Arrays.asList(path.split("/+")));
			if (segs.isEmpty()) throw new SolrServerException("Solr uri invalid: " + url);
			String core = segs.remove(segs.size() - 1);

			String base = url.replaceAll("/?" + core + "/?$", "");
			try {
				CoreAdminResponse resp = req.process(Solrs.open(base));
				String[] cores = new String[resp.getCoreStatus().size()];
				for (int i = 0; i < resp.getCoreStatus().size(); i++)
					cores[i] = resp.getCoreStatus().getName(i);
				return new Tuple3<>(base, core, cores);
			} catch (RemoteSolrException ee) {
				throw new SolrServerException("Solr uri base parsing failure: " + url);
			}
		}
	}
}
