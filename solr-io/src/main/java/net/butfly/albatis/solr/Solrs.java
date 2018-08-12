package net.butfly.albatis.solr;

import static net.butfly.albacore.paral.Sdream.of;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.util.Pair;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Utils;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.io.Rmap;

public final class Solrs extends Utils {
	protected static final Logger logger = Logger.getLogger(Solrs.class);
	private static final Map<String, SolrClient> clients = Maps.of();

	private Solrs() {}

	@SuppressWarnings("resource")
	@Deprecated
	public static SolrClient open(String solrURL) {
		try {
			return new SolrConnection(new URISpec(solrURL), false).client;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Deprecated
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

	@Deprecated
	public static SolrDocumentList query(String url, SolrQuery query) throws SolrServerException, IOException {
		return open(url).query(query).getResults();
	}

	@Deprecated
	public static QueryResponse queryForResponse(String url, SolrQuery query) throws SolrServerException, IOException {
		return open(url).query(query);
	}

	@Deprecated
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

	@Deprecated
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
	@Deprecated
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

	@Deprecated
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

	public static SolrInputDocument input(Rmap m, String keyFieldName) {
		if (null == m) return null;
		if (m.isEmpty() || null == m.key()) return null;
		SolrInputDocument doc = new SolrInputDocument(of(m).map(e -> {
			SolrInputField f = new SolrInputField(e.getKey());
			f.setValue(e.getValue());
			return f;
		}).partitions(SolrInputField::getName, f -> f));
		doc.addField(keyFieldName, m.key());
		return doc;
	}
}
