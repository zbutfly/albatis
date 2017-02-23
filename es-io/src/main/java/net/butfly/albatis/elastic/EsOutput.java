package net.butfly.albatis.elastic;

import static net.butfly.albacore.utils.Exceptions.unwrap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.transport.RemoteTransportException;

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
		super(name, b -> new ElasticMessage(b), failoverPath, Integer.parseInt(uri.getParameter("batch", "200")));
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

	@Override
	protected int write(String type, Collection<ElasticMessage> values) throws FailoverException {
		if (values.isEmpty()) return 0;
		Map<String, String> fails = new ConcurrentHashMap<>();
		long reqSize = 0;
		String sample = "";
		int succCount = 0;

		int retry = 0;
		List<ElasticMessage> retries = new ArrayList<>(values);
		long now = System.currentTimeMillis();
		for (; !retries.isEmpty() && retry < maxRetry; retry++) {
			BulkRequest req = new BulkRequest().add(IO.list(retries, ElasticMessage::forWrite));
			if (logger().isTraceEnabled() && retry == 0) reqSize = req.estimatedSizeInBytes();
			Map<Boolean, List<BulkItemResponse>> resps = null;
			try {
				resps = IO.collect(conn.client().bulk(req).actionGet(), Collectors.partitioningBy(r -> r.isFailed()));
			} catch (Exception e) {
				logger().warn("ES failure, request size [" + reqSize + "], retry...", unwrap(e));
			}
			if (resps != null) {
				if (resps.get(Boolean.TRUE).isEmpty()) {
					List<BulkItemResponse> s = resps.get(Boolean.TRUE);
					if (logger().isTraceEnabled()) logger().trace("Try#" + retry + " finished, total:[" + values.size() + "/" + Texts
							.formatKilo(reqSize, " bytes") + "] in [" + (System.currentTimeMillis() - now) + "] ms, sample id: [" + (s
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
				Set<String> succs = IO.map(resps.get(Boolean.FALSE), r -> r.getId(), Collectors.toSet());
				retries = IO.list(Streams.of(retries).filter(es -> !succs.contains(es.id) && !failing.containsKey(es.id)));
				fails.putAll(failing);
				sample = succs.isEmpty() ? null : succs.toArray(new String[succs.size()])[0];
				succCount += succs.size();
			}
		}
		if (logger().isTraceEnabled()) {
			logger().trace("Try#" + retry + " finished, total:[" + values.size() + "/" + Texts.formatKilo(reqSize, " bytes") + "] in ["
					+ (System.currentTimeMillis() - now) + "] ms, successed:[" + succCount + "], remained:[" + retries.size()
					+ "], failed:[" + fails.size() + "], sample id: [" + sample + "].");
		}
		if (!retries.isEmpty()) fails.putAll(IO.collect(retries, Collectors.toConcurrentMap(m -> m.id,
				m -> "VersionConfliction or EsRejected")));
		if (fails.isEmpty()) return values.size();
		else throw new FailoverException(IO.collect(Streams.of(values).filter(es -> fails.containsKey(es.id)), Collectors.toMap(es -> es,
				es -> fails.get(es.id))));
	}

	private String wrapErrorMessage(BulkItemResponse r) {
		return "Writing failed id [" + r.getId() + "] for: " + unwrap(r.getFailure().getCause()).getMessage();
	}

	static {
		unwrap(RemoteTransportException.class, "getCause");
	}
}
