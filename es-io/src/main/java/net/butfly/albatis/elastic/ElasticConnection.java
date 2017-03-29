package net.butfly.albatis.elastic;

import static net.butfly.albacore.utils.Exceptions.unwrap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
	public long update(String type, Stream<ElasticMessage> msgs, Map<ElasticMessage, String> fails) {
		List<ElasticMessage> values = IO.list(msgs);
		if (values.isEmpty()) return 0;
		if (values.size() == 1) return update(type, values.get(0)) ? 1 : 0;
		Map<String, String> failIds = new ConcurrentHashMap<>();
		long totalReqBytes = 0;
		String sample = "";
		int succCount = 0;

		int retry = 0;
		List<ElasticMessage> retries = new ArrayList<>(values);
		long now = System.currentTimeMillis();
		for (; !retries.isEmpty() && retry < maxRetry; retry++) {
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
				Stream<BulkItemResponse> ss = Streams.of(resps.get(Boolean.TRUE)) //
						// XXX: disable retry, damn slowly!
						.filter(r -> {
							@SuppressWarnings("unchecked")
							Class<Throwable> c = (Class<Throwable>) r.getFailure().getCause().getClass();
							return EsRejectedExecutionException.class.isAssignableFrom(c) || VersionConflictEngineException.class
									.isAssignableFrom(c);
						});
				Map<String, String> failing = IO.collect(ss, Collectors.toConcurrentMap(r -> r.getFailure().getId(),
						r -> "Writing failed id [" + r.getId() + "] for: " + unwrap(r.getFailure().getCause()).getMessage()));
				Set<String> succs = IO.collect(resps.get(Boolean.FALSE), r -> r.getId(), Collectors.toSet());
				retries = IO.list(Streams.of(retries).filter(e -> !succs.contains(e.id) && !failing.containsKey(e.id)));
				failIds.putAll(failing);
				sample = succs.isEmpty() ? null : succs.toArray(new String[succs.size()])[0];
				succCount += succs.size();
			}
		}
		if (logger().isTraceEnabled()) logger().debug("Try#" + retry + " finished, total:[" + values.size() + "/" + Texts.formatKilo(
				totalReqBytes, " bytes") + "] in [" + (System.currentTimeMillis() - now) + "] ms, successed:[" + succCount + "], remained:["
				+ retries.size() + "], failed:[" + failIds.size() + "], sample id: [" + sample + "].");
		if (!retries.isEmpty()) failIds.putAll(IO.collect(retries, Collectors.toConcurrentMap(m -> m.id,
				m -> "VersionConfliction or EsRejected")));
		if (!failIds.isEmpty()) fails.putAll(IO.map(Streams.of(values).filter(e -> failIds.containsKey(e.id)), e -> e, e -> failIds.get(
				e.id)));
		return values.size() - failIds.size();
	}
}
