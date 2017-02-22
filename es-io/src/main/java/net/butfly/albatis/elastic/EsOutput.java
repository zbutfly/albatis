package net.butfly.albatis.elastic;

import static net.butfly.albacore.utils.Exceptions.unwrap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.transport.RemoteTransportException;

import net.butfly.albacore.io.Streams;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.io.faliover.Failover.FailoverException;
import net.butfly.albacore.io.faliover.FailoverOutput;
import net.butfly.albacore.utils.Configs;

public final class EsOutput extends FailoverOutput<String, ElasticMessage> {
	private final static int PACKAGE_SIZE = Integer.parseInt(Configs.MAIN_CONF.getOrDefault("albatis.io.es.batch.size", "100"));
	private final ElasticConnection conn;
	private final int maxRetry;

	public EsOutput(String name, String esUri, String failoverPath) throws IOException {
		this(name, new URISpec(esUri), failoverPath);
	}

	public EsOutput(String name, URISpec esURI, String failoverPath) throws IOException {
		super(name, b -> new ElasticMessage(b), failoverPath, PACKAGE_SIZE);
		conn = new ElasticConnection(esURI);
		maxRetry = Integer.parseInt(esURI.getParameter("retry", "5"));
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
		int tc = values.size();
		Map<String, String> fails = new ConcurrentHashMap<>();
		List<ElasticMessage> retries = new ArrayList<>(values);
		AtomicInteger t = new AtomicInteger();
		Set<String> succs = new ConcurrentSkipListSet<>();
		do {
			long now = System.currentTimeMillis();
			BulkRequest req = new BulkRequest().add(io.list(retries, ElasticMessage::forWrite));
			try {
				Map<Boolean, List<BulkItemResponse>> resps = io.collect(conn.client().bulk(req).actionGet(), Collectors.partitioningBy(
						r -> r.isFailed()));
				if (resps.get(Boolean.TRUE).isEmpty()) return resps.get(Boolean.FALSE).size();
				succs.clear();
				io.each(resps.get(Boolean.FALSE), r -> succs.add(r.getId()));
				Map<String, String> failing = io.collect(Streams.of(resps.get(Boolean.TRUE)) //
						// XXX: disable retry, damn slowly!
						.filter(r -> {
							@SuppressWarnings("unchecked")
							Class<Throwable> c = (Class<Throwable>) r.getFailure().getCause().getClass();
							return EsRejectedExecutionException.class.isAssignableFrom(c) || VersionConflictEngineException.class
									.isAssignableFrom(c);
						})//
						, Collectors.toConcurrentMap(r -> r.getFailure().getId(), this::wrapErrorMessage));
				retries = io.list(Streams.of(retries).filter(es -> !succs.contains(es.id) && !failing.containsKey(es.id)));
				fails.putAll(failing);
			} catch (Exception e) {
				throw new RuntimeException(unwrap(e));
			} finally {
				if (logger().isTraceEnabled()) {
					int rc = retries.size();
					long reqs = req.estimatedSizeInBytes();
					String sample = succs.isEmpty() ? null : new ArrayList<>(succs).get(0);
					logger().trace(() -> "Retry#" + t.get() + ": [" + reqs + "] bytes in [" + (System.currentTimeMillis() - now)
							+ "] ms, successed:[" + succs.size() + "], remained:[" + rc + "], failed:[" + fails.size() + "], total:[" + tc
							+ "], sample id: [" + sample + "].");
				}
			}
		} while (!retries.isEmpty() && t.getAndIncrement() < maxRetry);
		if (!retries.isEmpty()) fails.putAll(io.collect(retries, Collectors.toConcurrentMap(m -> m.id,
				m -> "VersionConfliction or EsRejected")));
		if (fails.isEmpty()) return tc;
		else throw new FailoverException(io.collect(Streams.of(values).filter(es -> fails.containsKey(es.id)), Collectors.toMap(es -> es,
				es -> fails.get(es.id))));
	}

	private String wrapErrorMessage(BulkItemResponse r) {
		return "Writing failed id [" + r.getId() + "] for: " + unwrap(r.getFailure().getCause()).getMessage();
	}

	static {
		unwrap(RemoteTransportException.class, "getCause");
	}
}
