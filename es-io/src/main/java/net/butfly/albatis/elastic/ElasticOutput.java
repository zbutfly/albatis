package net.butfly.albatis.elastic;

import static net.butfly.albacore.utils.Exceptions.unwrap;
import static net.butfly.albacore.utils.collection.Streams.collect;
import static net.butfly.albacore.utils.collection.Streams.list;
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
import net.butfly.albacore.utils.Texts;
import net.butfly.albacore.utils.collection.Streams;
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
	public final long enqueue(Stream<Message> msgs) throws EnqueueException {
		ConcurrentMap<String, Message> origin = msgs.filter(Streams.NOT_NULL).collect(Collectors.toConcurrentMap(m -> m.key(), m -> m));
		if (origin.isEmpty()) return 0;
		int originSize = origin.size();
		int retry = 0;
		while (!origin.isEmpty() && retry++ <= maxRetry) {
			@SuppressWarnings("rawtypes")
			List<DocWriteRequest> reqs = list(origin.values(), Elastics::forWrite);
			if (reqs.isEmpty()) return 0;
			BulkRequest req = new BulkRequest().add(reqs);
			long bytes = logger().isTraceEnabled() ? req.estimatedSizeInBytes() : 0;
			long now = System.currentTimeMillis();
			int currentRetry = retry;
			try {
				conn.client().bulk(req, new ActionListener<BulkResponse>() {
					@Override
					public void onResponse(BulkResponse response) {
						Map<Boolean, List<BulkItemResponse>> resps = collect(response, Collectors.partitioningBy(r -> r.isFailed()));
						List<BulkItemResponse> succResps = resps.get(Boolean.FALSE);
						List<BulkItemResponse> failResps = resps.get(Boolean.TRUE);
						String sample = succResps.isEmpty() ? null : succResps.get(0).getId();
						Result result = null;
						eex.success(succResps.size());
						if (failResps.isEmpty()) {//
							origin.clear();
							result = new Result(succResps.size(), 0, sample);
						} else {
							// process success: remove from origin
							succResps.forEach(succ -> origin.remove(succ.getId()));
							// process failing and retry...
							Map<Boolean, List<BulkItemResponse>> failOrRetry = of(failResps).collect(Collectors.partitioningBy(
									ElasticOutput.this::failed));
							failOrRetry.get(Boolean.TRUE).forEach(r -> {
								Message m = origin.remove(r.getId());
								Exception cause = r.getFailure().getCause();
								if (null != m) eex.fail(m, cause);
								else logger().debug("Message [" + r.getId() + "] could not failover for [" + cause.getMessage()
										+ "], maybe processed or lost");
							});
							Set<String> failIds = collect(failOrRetry.get(Boolean.TRUE), r -> r.getFailure().getId(), Collectors.toSet());
							if (logger().isDebugEnabled()) //
								failOrRetry.get(Boolean.TRUE).forEach(r -> logger().warn("Writing failed id [" + r.getId() + "] for: "
										+ unwrap(r.getFailure().getCause()).getMessage()));

							// process and stats success...
							result = new Result(succResps.size(), failIds.size(), sample);
						}
						if (failResps.isEmpty()) return;
						// process success: remove from origin
						succResps.forEach(succ -> origin.remove(succ.getId()));
						// process failing and retry...
						Map<Boolean, List<BulkItemResponse>> failOrRetry = of(failResps).collect(Collectors.partitioningBy(r -> noRetry(r
								.getFailure().getCause())));
						failed(failOrRetry.get(Boolean.FALSE).parallelStream().map(r -> origin.remove(r.getId())));
						if (logger().isTraceEnabled()) //
							logger.warn(() -> "Some fails: \n" + failOrRetry.get(Boolean.TRUE).parallelStream().map(r -> "\tfailed id [" + r
									.getFailure().getId() + "] for: " + unwrap(r.getFailure().getCause()).toString()).collect(Collectors
											.joining("\n")));
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

	static {
		unwrap(RemoteTransportException.class, "getCause");
	}
}
