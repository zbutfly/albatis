package net.butfly.albatis.elastic;

import static net.butfly.albacore.utils.Exceptions.unwrap;
import static net.butfly.albacore.utils.collection.Streams.collect;
import static net.butfly.albacore.utils.collection.Streams.list;
import static net.butfly.albacore.utils.collection.Streams.map;
import static net.butfly.albacore.utils.collection.Streams.of;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.mapper.MapperException;
import org.elasticsearch.transport.RemoteTransportException;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.utils.Exceptions;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.io.Message;
import net.butfly.albatis.io.Output;

public final class ElasticOutput extends Namedly implements Output<Message> {
	private static final Logger logger = Logger.getLogger(ElasticOutput.class);
	public static final int SUGGEST_RETRY = 5;
	public static final int SUGGEST_BATCH_SIZE = 1000;
	private final ElasticConnection conn;
	private final int maxRetry;

	public ElasticOutput(String name, ElasticConnection conn) throws IOException {
		super(name);
		this.conn = conn;
		maxRetry = SUGGEST_RETRY;
		open();
	}

	@Override
	public void close() {
		try {
			conn.close();
		} catch (Exception e) {}
	}

	@Override
	public final void enqueue(Stream<Message> msgs) {
		ConcurrentMap<String, Message> origin = collect(msgs, Collectors.toConcurrentMap(Message::key, conn::fixTable, (m1, m2) -> {
			logger.trace(() -> "Duplicated key [" + m1.key() + "], \n\t" + m1.toString() + "\ncoverd\n\t" + m2.toString());
			return m1;
		}));
		if (origin.isEmpty()) return;
		int retry = 0;
		while (!origin.isEmpty() && retry++ <= maxRetry) {
			@SuppressWarnings("rawtypes")
			List<DocWriteRequest> reqs = list(origin.values(), Elastics::forWrite);
			if (reqs.isEmpty()) return;
			BulkRequest req = new BulkRequest().add(reqs);
			try {
				conn.client().bulk(req, new ActionListener<BulkResponse>() {
					@Override
					public void onResponse(BulkResponse response) {
						Map<Boolean, List<BulkItemResponse>> resps = collect(response, Collectors.partitioningBy(r -> r.isFailed()));
						List<BulkItemResponse> succResps = resps.get(Boolean.FALSE);
						List<BulkItemResponse> failResps = resps.get(Boolean.TRUE);
						if (!succResps.isEmpty()) {
							logger.trace(() -> "Elastic enqueue succeed [" + req.estimatedSizeInBytes() + " bytes], sample id: " + succResps
									.get(0).getId());
							succeeded(succResps.size());
						}
						if (failResps.isEmpty()) return;
						// process success: remove from origin
						succResps.forEach(succ -> origin.remove(succ.getId()));
						// process failing and retry...
						Map<Boolean, List<BulkItemResponse>> failOrRetry = of(failResps).collect(Collectors.partitioningBy(r -> noRetry(r
								.getFailure().getCause())));
						List<BulkItemResponse> ll = failOrRetry.get(Boolean.FALSE);
						failed(map(ll, r -> origin.remove(r.getId())));
						if (logger().isTraceEnabled()) logger.warn(() -> "Some fails: \n" + map(failOrRetry.get(Boolean.TRUE),
								r -> "\tfailed id [" + r.getFailure().getId() + "] for: " + unwrap(r.getFailure().getCause()).toString(),
								Collectors.joining("\n")));
					}

					@Override
					public void onFailure(Exception e) {
						Throwable t = Exceptions.unwrap(e);
						if (noRetry(t)) logger().warn("Elastic connection op failed [" + t + "], [" + origin.size() + "] fails", t);
						else failed(origin.values().parallelStream());
					}
				});
			} catch (IllegalStateException ex) {
				logger().error("Elastic client fail: [" + ex.toString() + "]");
				// failed(origin.values().parallelStream());
			}
		}
	}

	private boolean noRetry(Throwable cause) {
		while (RemoteTransportException.class.isAssignableFrom(cause.getClass()) && cause.getCause() != null)
			cause = cause.getCause();
		if (MapperException.class.isAssignableFrom(cause.getClass())) logger().error("ES mapper exception", cause);
		return EsRejectedExecutionException.class.isAssignableFrom(cause.getClass())//
				// || VersionConflictEngineException.class.isAssignableFrom(c)
				|| MapperException.class.isAssignableFrom(cause.getClass());
	}

	private boolean failed(BulkItemResponse r) {
		Throwable cause = r.getFailure().getCause();
		return failed(cause);
	}

	private boolean failed(Throwable cause) {
		while (RemoteTransportException.class.isAssignableFrom(cause.getClass()) && cause.getCause() != null)
			cause = cause.getCause();
		if (MapperException.class.isAssignableFrom(cause.getClass())) logger().error("ES mapper exception", cause);
		return EsRejectedExecutionException.class.isAssignableFrom(cause.getClass())//
				// || VersionConflictEngineException.class.isAssignableFrom(c)
				|| MapperException.class.isAssignableFrom(cause.getClass());
	}

	static {
		unwrap(RemoteTransportException.class, "getCause");
	}
}
