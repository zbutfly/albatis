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
import java.util.concurrent.atomic.AtomicLong;
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
import net.butfly.albacore.utils.Texts;

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
		AtomicInteger retry = new AtomicInteger(0);
		Set<String> succs = new ConcurrentSkipListSet<>();
		long now = System.currentTimeMillis();
		AtomicLong reqSize = new AtomicLong(0);
		try {
			for (; !retries.isEmpty() && retry.getAndIncrement() < maxRetry;) {
				BulkRequest req = new BulkRequest().add(io.list(retries, ElasticMessage::forWrite));
				if (logger().isTraceEnabled()) logger().trace("BulkRequesting#" + retry.get() + ": " + retries.size() + " requests/" + req
						.estimatedSizeInBytes() + " bytes, sample: " + retries.get(0).toString() + ".");
				Map<Boolean, List<BulkItemResponse>> resps = null;
				try {
					resps = io.collect(conn.client().bulk(req).actionGet(), Collectors.partitioningBy(r -> r.isFailed()));
				} catch (Exception e) {
					logger().warn("ES failure, request size [" + reqSize.get() + "], retry...", unwrap(e));
				}
				if (resps != null) {
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
				}
			}
		} finally {
			if (logger().isTraceEnabled()) {
				String sample = succs.isEmpty() ? "NONE" : new ArrayList<>(succs).get(0);
				logger().trace("Try#" + retry.get() + ", total:[" + tc + "/" + Texts.formatKilo(reqSize.get(), " bytes") + "] in ["
						+ (System.currentTimeMillis() - now) + "] ms, successed:[" + succs.size() + "], remained:[" + retries.size()
						+ "], failed:[" + fails.size() + "], sample id: [" + sample + "].");
			}
		}
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
