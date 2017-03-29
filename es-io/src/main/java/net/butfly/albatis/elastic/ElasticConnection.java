package net.butfly.albatis.elastic;

import static net.butfly.albacore.utils.Exceptions.unwrap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.engine.VersionConflictEngineException;

import com.hzcominfo.albatis.nosql.NoSqlConnection;

import net.butfly.albacore.io.IO;
import net.butfly.albacore.io.utils.Streams;
import net.butfly.albacore.io.utils.URISpec;
import net.butfly.albacore.utils.Texts;

public class ElasticConnection extends NoSqlConnection<TransportClient> implements ElasticConnect {
	private final int maxRetry;

	public ElasticConnection(URISpec uri, Map<String, String> props) throws IOException {
		super(uri, u -> ElasticConnect.Builder.buildTransportClient(u, props), 39300, "es", "elasticsearch");
		this.maxRetry = Integer.parseInt(uri.getParameter("retry", "5"));
	}

	public ElasticConnection(URISpec uri) throws IOException {
		this(uri, null);
	}

	public ElasticConnection(String url, Map<String, String> props) throws IOException {
		this(new URISpec(url), props);
	}

	public ElasticConnection(String url) throws IOException {
		this(new URISpec(url));
	}

	@Override
	public String getDefaultIndex() {
		return uri.getPathAt(0);
	}

	@Override
	public String getDefaultType() {
		return uri.getFile();
	}

	@Override
	public void mapping(Map<String, Object> mapping, String... types) {
		logger().debug("Mapping constructing: " + mapping);
		PutMappingRequest req = new PutMappingRequest(getDefaultIndex());
		req.source(mapping);
		Set<String> tps = new HashSet<>(Arrays.asList(types));
		if (null != getDefaultType()) tps.add(getDefaultType());
		for (String t : tps)
			if (!client().admin().indices().putMapping(req.type(t)).actionGet().isAcknowledged()) logger().error("Mapping failed on type ["
					+ t + "]" + req.toString());
	}

	@Override
	public void close() {
		try {
			super.close();
		} catch (IOException e) {
			logger().error("Close failure", e);
		}
		client().close();
	}

	private <T> T get(ActionFuture<T> f, int retry) {
		try {
			return f.actionGet();
		} catch (Exception e) {
			logger().warn("ES failure, retry#" + retry + "...", unwrap(e));
		} finally {}
		return null;
	}

	@Override
	public boolean update(String type, ElasticMessage es) {
		if (es.type == null) es.type = type;
		ActionRequest req = es.forWrite();
		if (req instanceof IndexRequest) get(client().index((IndexRequest) req), 0);
		else if (req instanceof UpdateRequest) get(client().update((UpdateRequest) req), 0);
		else return false;
		return true;
	}

	@Override
	public long update(String type, Stream<ElasticMessage> msgs, Set<ElasticMessage> fails) {
		List<ElasticMessage> values = IO.list(msgs);
		if (values.isEmpty()) return 0;
		if (values.size() == 1) return update(type, values.get(0)) ? 1 : 0;
		long totalReqBytes = 0;
		String sample = "";
		int succCount = 0;

		int retry = 0;
		List<ElasticMessage> retries = new ArrayList<>(values);
		for (; !retries.isEmpty() && retry < maxRetry; retry++) {
			long now = System.currentTimeMillis();
			BulkRequest req = new BulkRequest().add(IO.list(retries, ElasticMessage::forWrite));
			if (logger().isTraceEnabled() && retry == 0) totalReqBytes = req.estimatedSizeInBytes();
			ActionFuture<BulkResponse> future = client().bulk(req);
			BulkResponse rg = get(future, retry);
			if (rg != null) {
				Map<Boolean, List<BulkItemResponse>> resps = IO.collect(rg, Collectors.partitioningBy(r -> r.isFailed()));
				if (resps.get(Boolean.TRUE).isEmpty()) {
					List<BulkItemResponse> s = resps.get(Boolean.FALSE);
					if (logger().isTraceEnabled()) logger().debug("Try#" + retry + " finished, total:[" + values.size() + "/" + Texts
							.formatKilo(totalReqBytes, " bytes") + "] in [" + (System.currentTimeMillis() - now) + "] ms, sample id: [" + (s
									.isEmpty() ? "NONE" : s.get(0).getId()) + "].");
					return s.size();
				}
				Set<String> succs = IO.collect(resps.get(Boolean.FALSE), r -> r.getId(), Collectors.toSet());

				// process fails and retry...
				Stream<BulkItemResponse> sresp = Streams.of(resps.get(Boolean.TRUE)) //
						// XXX: disable retry, damn slowly!
						.filter(r -> {
							@SuppressWarnings("unchecked")
							Class<Throwable> c = (Class<Throwable>) r.getFailure().getCause().getClass();
							return EsRejectedExecutionException.class.isAssignableFrom(c) || VersionConflictEngineException.class
									.isAssignableFrom(c);
						});
				if (logger().isDebugEnabled()) sresp = sresp.peek(r -> logger().warn("Writing failed id [" + r.getId() + "] for: " + unwrap(
						r.getFailure().getCause()).getMessage()));
				Set<String> failingIds = IO.collect(sresp.map(r -> r.getFailure().getId()), Collectors.toSet());
				// remove from retries: successed or marked fail.
				retries = IO.list(Streams.of(retries).filter(e -> !succs.contains(e.id) && !failingIds.contains(e.id)));
				// move fail marked into fails
				fails.addAll(IO.list(failingIds, id -> {
					for (ElasticMessage v : values)
						if (id.equals(v.id)) return v;
					return null;
				}));

				// process and stats success...
				sample = succs.isEmpty() ? null : succs.toArray(new String[succs.size()])[0];
				succCount += succs.size();
				if (logger().isTraceEnabled()) logger().debug("Try#" + retry + " finished, total:[" + succs.size() + "/" + Texts.formatKilo(
						totalReqBytes, " bytes") + "] in [" + (System.currentTimeMillis() - now) + "] ms, total successed:[" + succCount
						+ "], remained:[" + retries.size() + "], failed:[" + failingIds.size() + "], sample id: [" + sample + "].");
			}
		}

		if (!retries.isEmpty()) fails.addAll(retries);
		return succCount;
	}
}
