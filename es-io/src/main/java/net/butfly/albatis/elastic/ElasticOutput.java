package net.butfly.albatis.elastic;

import static net.butfly.albacore.utils.Exceptions.unwrap;
import static net.butfly.albacore.utils.collection.Streams.collect;
import static net.butfly.albacore.utils.collection.Streams.list;
import static net.butfly.albacore.utils.collection.Streams.of;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
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
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albacore.utils.parallel.Concurrents;
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
		ConcurrentMap<String, Message> origin = msgs.filter(Streams.NOT_NULL).collect(Collectors.toConcurrentMap(Message::key,
				conn::fixTable, (m1, m2) -> {
					logger.trace(() -> "Duplicated key [" + m1.key() + "], \n\t" + m1.toString() + "\ncoverd\n\t" + m2.toString());
					return m1;
				}));
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
			AtomicBoolean finished = new AtomicBoolean(false);
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
							failOrRetry.get(Boolean.FALSE).forEach(r -> {
								Message m = origin.remove(r.getId());
								Exception cause = r.getFailure().getCause();
								if (null != m) eex.fail(m, cause);
								else logger().debug("Message [" + r.getId() + "] not found in origin, but failed for [" + cause.toString()
										+ "], maybe processed or lost");
							});
							Set<String> failIds = collect(failOrRetry.get(Boolean.TRUE), r -> r.getFailure().getId(), Collectors.toSet());
							if (logger().isTraceEnabled()) //
								failOrRetry.get(Boolean.TRUE).forEach(r -> logger().warn("Writing failed id [" + r.getId() + "] for: "
										+ unwrap(r.getFailure().getCause()).toString()));

							// process and stats success...
							result = new Result(succResps.size(), failIds.size(), sample);
						}
						if (result != null && logger().isTraceEnabled()) result.trace(origin.size(), bytes, currentRetry, origin.size(),
								System.currentTimeMillis() - now);
						finished.set(true);
					}

					@Override
					public void onFailure(Exception e) {
						Throwable t = Exceptions.unwrap(e);
						if (failed(t)) logger().warn("Elastic connection op failed [" + t + "], [" + origin.size() + "] fails", t);
						else eex.fails(origin.values());
						finished.set(true);
					}
				});
				while (!finished.get())
					Concurrents.waitSleep(100);

			} catch (IllegalStateException ex) {
				logger().error("Elastic client fail: [" + ex.toString() + "]");
				eex.fails(origin.values());
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
