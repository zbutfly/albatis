package net.butfly.albatis.elastic;

import static net.butfly.albacore.utils.Exceptions.unwrap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.transport.RemoteTransportException;

import com.hzcominfo.albatis.nosql.Connection;

import net.butfly.albacore.io.IO;
import net.butfly.albacore.io.Streams;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.io.faliover.Failover.FailoverException;
import net.butfly.albacore.io.faliover.FailoverOutput;
import net.butfly.albacore.utils.Texts;

public final class EsOutput extends FailoverOutput<String, ElasticMessage> {
	private final ElasticConnection conn;
	private final int maxRetry;

	public EsOutput(String name, String uri, String failoverPath) throws IOException {
		this(name, new URISpec(uri), failoverPath);
	}

	public EsOutput(String name, URISpec uri, String failoverPath) throws IOException {
		super(name, b -> new ElasticMessage(b), failoverPath, Integer.parseInt(uri.getParameter(Connection.PARAM_KEY_BATCH, "200")));
		conn = new ElasticConnection(uri);
		maxRetry = Integer.parseInt(uri.getParameter("retry", "5"));
		open();
	}

	public ElasticConnection getConnection() {
		return conn;
	}

	@Override
	protected void closeInternal() {
		conn.close();
	}

	private long write(String type, ElasticMessage es) {
		ActionRequest<?> req = es.forWrite();
		ActionFuture<? extends ActionWriteResponse> f = null;
		if (req instanceof IndexRequest) f = conn.client().index((IndexRequest) req);
		else if (req instanceof UpdateRequest) f = conn.client().update((UpdateRequest) req);
		if (null == f) return 0;
		@SuppressWarnings("unused")
		ActionWriteResponse r = get(f, 0);
		return 1;
	}

	@Override
	protected long write(String type, Stream<ElasticMessage> msgs) throws FailoverException {
		List<ElasticMessage> values = IO.list(msgs);
		if (values.isEmpty()) return 0;
		if (values.size() == 1) return write(type, values.get(0));
		Map<String, String> fails = new ConcurrentHashMap<>();
		long totalReqBytes = 0;
		String sample = "";
		int succCount = 0;

		int retry = 0;
		List<ElasticMessage> retries = new ArrayList<>(values);
		long now = System.currentTimeMillis();
		for (; !retries.isEmpty() && retry < maxRetry; retry++) {
			BulkRequest req = new BulkRequest().add(IO.list(retries, ElasticMessage::forWrite));
			if (logger().isTraceEnabled() && retry == 0) totalReqBytes = req.estimatedSizeInBytes();
			ActionFuture<BulkResponse> future = conn.client().bulk(req);
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
				Map<String, String> failing = IO.collect(Streams.of(resps.get(Boolean.TRUE)) //
						// XXX: disable retry, damn slowly!
						.filter(r -> {
							@SuppressWarnings("unchecked")
							Class<Throwable> c = (Class<Throwable>) r.getFailure().getCause().getClass();
							return EsRejectedExecutionException.class.isAssignableFrom(c) || VersionConflictEngineException.class
									.isAssignableFrom(c);
						})//
						, Collectors.toConcurrentMap(r -> r.getFailure().getId(), this::wrapErrorMessage));
				Set<String> succs = IO.collect(resps.get(Boolean.FALSE), r -> r.getId(), Collectors.toSet());
				retries = IO.list(Streams.of(retries).filter(es -> !succs.contains(es.id) && !failing.containsKey(es.id)));
				fails.putAll(failing);
				sample = succs.isEmpty() ? null : succs.toArray(new String[succs.size()])[0];
				succCount += succs.size();
			}
		}
		if (logger().isTraceEnabled()) {
			logger().debug("Try#" + retry + " finished, total:[" + values.size() + "/" + Texts.formatKilo(totalReqBytes, " bytes") + "] in ["
					+ (System.currentTimeMillis() - now) + "] ms, successed:[" + succCount + "], remained:[" + retries.size()
					+ "], failed:[" + fails.size() + "], sample id: [" + sample + "].");
		}
		if (!retries.isEmpty()) fails.putAll(IO.collect(retries, Collectors.toConcurrentMap(m -> m.id,
				m -> "VersionConfliction or EsRejected")));
		if (fails.isEmpty()) return values.size();
		else throw new FailoverException(IO.map(Streams.of(values).filter(es -> fails.containsKey(es.id)), es -> es, es -> fails.get(
				es.id)));
	}

	private String wrapErrorMessage(BulkItemResponse r) {
		return "Writing failed id [" + r.getId() + "] for: " + unwrap(r.getFailure().getCause()).getMessage();
	}

	static {
		unwrap(RemoteTransportException.class, "getCause");
	}

	private <T> T get(ActionFuture<T> f, int retry) {
		try {
			return f.actionGet();
		} catch (Exception e) {
			logger().warn("ES failure, retry#" + retry + "...", unwrap(e));
		} finally {
		}
		return null;
	}
}
