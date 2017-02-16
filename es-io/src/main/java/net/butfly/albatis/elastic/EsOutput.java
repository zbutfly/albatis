package net.butfly.albatis.elastic;

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
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.transport.RemoteTransportException;

import net.butfly.albacore.io.Streams;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.io.faliover.Failover.FailoverException;
import net.butfly.albacore.io.faliover.FailoverOutput;
import static net.butfly.albacore.utils.Exceptions.unwrap;

public final class EsOutput extends FailoverOutput<ElasticMessage> {
	private final ElasticConnection conn;

	public EsOutput(String name, String esUri, String failoverPath) throws IOException {
		super(name, b -> new ElasticMessage(b), failoverPath, 100, 20);
		conn = new ElasticConnection(esUri);
		open();
	}

	public EsOutput(String name, URISpec esURI, String failoverPath) throws IOException {
		super(name, b -> new ElasticMessage(b), failoverPath, 100, 20);
		conn = new ElasticConnection(esURI);
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
		List<ElasticMessage> retries = new ArrayList<>(values);
		do {
			BulkRequest req = new BulkRequest().add(io.list(retries, ElasticMessage::forWrite));
			logger().trace("Bulk size: " + req.estimatedSizeInBytes());
			try {
				TransportClient tc = conn.client();
				BulkResponse bulk = tc.bulk(req).actionGet();
				Map<Boolean, List<BulkItemResponse>> resps = io.collect(bulk, Collectors.partitioningBy(r -> r.isFailed()));
				if (resps.get(Boolean.TRUE).isEmpty()) return resps.get(Boolean.FALSE).size();
				Set<String> succs = io.map(resps.get(Boolean.FALSE), r -> r.getId(), Collectors.toSet());
				Map<Boolean, List<BulkItemResponse>> retryMap = io.collect(Streams.of(resps.get(Boolean.TRUE)), Collectors.partitioningBy(
						r -> {
							@SuppressWarnings("unchecked")
							Class<Throwable> c = (Class<Throwable>) r.getFailure().getCause().getClass();
							return EsRejectedExecutionException.class.isAssignableFrom(c) || VersionConflictEngineException.class
									.isAssignableFrom(c);
						}));
				Map<String, String> failing = io.collect(retryMap.get(Boolean.FALSE), Collectors.toConcurrentMap(r -> r.getFailure()
						.getId(), this::wrapErrorMessage));
				retries = io.list(Streams.of(retries).filter(es -> !succs.contains(es.id) && !failing.containsKey(es.id)));
				fails.putAll(failing);
			} catch (Exception e) {
				throw new RuntimeException(unwrap(e));
			}
		} while (!retries.isEmpty());
		if (fails.isEmpty()) return values.size();
		else {
			throw new FailoverException(io.collect(Streams.of(values).filter(es -> fails.containsKey(es.id)), Collectors.toMap(es -> es,
					es -> fails.get(es.id))));
		}
	}

	private String wrapErrorMessage(BulkItemResponse r) {
		return "Writing failed id [" + r.getId() + "] for: " + unwrap(r.getFailure().getCause()).getMessage();
	}

	static {
		unwrap(RemoteTransportException.class, "getCause");
	}
}
