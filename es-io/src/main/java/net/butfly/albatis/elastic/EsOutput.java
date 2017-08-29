package net.butfly.albatis.elastic;

import static net.butfly.albacore.io.utils.Streams.collect;
import static net.butfly.albacore.io.utils.Streams.list;
import static net.butfly.albacore.io.utils.Streams.of;
import static net.butfly.albacore.utils.Exceptions.unwrap;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.mapper.MapperException;
import org.elasticsearch.transport.RemoteTransportException;

import com.hzcominfo.albatis.nosql.Connection;

import net.butfly.albacore.io.Message;
import net.butfly.albacore.io.faliover.FailoverOutput;
import net.butfly.albacore.io.utils.URISpec;
import net.butfly.albacore.utils.Exceptions;
import net.butfly.albacore.utils.Texts;

public final class EsOutput extends FailoverOutput {
	public static final long DEFAULT_BATCH_SIZE = 1000;
	private final ElasticConnection conn;
	private final int maxRetry;

	public EsOutput(String name, String uri, String failoverPath) throws IOException {
		this(name, new URISpec(uri), failoverPath);
	}

	public EsOutput(String name, String uri, String failoverPath, long batchSize) throws IOException {
		this(name, new URISpec(uri), failoverPath, batchSize);
	}

	public EsOutput(String name, URISpec uri, String failoverPath) throws IOException {
		this(name, uri, failoverPath, Integer.parseInt(uri.getParameter(Connection.PARAM_KEY_BATCH, "0")));
	}

	public EsOutput(String name, URISpec uri, String failoverPath, long batchSize) throws IOException {
		super(name, ElasticMessage.class, failoverPath, (int) (batchSize > 0 ? batchSize : DEFAULT_BATCH_SIZE));
		conn = new ElasticConnection(uri);
		maxRetry = Integer.parseInt(uri.getParameter("retry", "5"));
		open();
	}

	public ElasticConnect getConnection() {
		return conn;
	}

	@Override
	protected void closeInternal() {
		try {
			conn.close();
		} catch (Exception e) {}
	}

	@Override
	protected <M extends Message> long write(String type, Stream<M> msgs, Consumer<Collection<M>> failing, Consumer<Long> committing,
			int retry) {
		Map<String, M> origin = new ConcurrentHashMap<>();
		msgs.forEach(m -> origin.put(m.key(), m));
		long s = origin.size();
		if (s == 0) return 0;
		if (retry >= maxRetry) {
			failing.accept(origin.values());
			return 0;
		}
		if (s == 1) {
			long i = 0;
			for (M m : origin.values())
				if (update(type, (ElasticMessage) m)) i++;
				else failing.accept(Arrays.asList(m));
			committing.accept(i);
			return i;
		}
		LinkedBlockingQueue<M> retries = new LinkedBlockingQueue<>(origin.values());
		BulkRequest req = new BulkRequest().add(list(retries, m -> ((ElasticMessage) m).forWrite()));
		long bytes = logger().isTraceEnabled() ? req.estimatedSizeInBytes() : 0;
		long now = System.currentTimeMillis();

		try {
			conn.client().bulk(req, new ActionListener<BulkResponse>() {
				@Override
				public void onResponse(BulkResponse response) {
					Map<Boolean, List<BulkItemResponse>> resps = collect(response, Collectors.partitioningBy(r -> r.isFailed()));
					List<BulkItemResponse> succResps = resps.get(Boolean.FALSE);
					List<BulkItemResponse> failResps = resps.get(Boolean.TRUE);
					String sample = succResps.isEmpty() ? null : succResps.get(0).getId();
					Result result = null;
					if (failResps.isEmpty()) {//
						retries.clear();
						committing.accept((long) origin.size());
						result = new Result(succResps.size(), 0, sample);
					} else {
						Set<String> succIds = collect(succResps, r -> r.getId(), Collectors.toSet());
						// process failing and retry...
						Map<Boolean, List<BulkItemResponse>> failOrRetry = of(failResps).collect(Collectors.partitioningBy(
								EsOutput.this::failed));
						Set<String> failIds = collect(failOrRetry.get(Boolean.TRUE), r -> r.getFailure().getId(), Collectors.toSet());
						if (logger().isDebugEnabled()) //
							failOrRetry.get(Boolean.TRUE).forEach(r -> logger().warn("Writing failed id [" + r.getId() + "] for: " + unwrap(
									r.getFailure().getCause()).getMessage()));
						// remove from retries: successed or marked fail.
						retries.removeIf(m -> succIds.contains(m.key()) || failIds.contains(m.key()));
						// move fail marked into failing
						failing.accept((list(failIds, origin::get)));

						// process and stats success...
						result = new Result(succIds.size(), failIds.size(), sample);
					}
					if (result != null && logger().isTraceEnabled()) result.trace(origin.size(), bytes, retry, retries.size(), System
							.currentTimeMillis() - now);
					if (!retries.isEmpty()) write(type, of(retries), failing, committing, retry + 1);
					logger().trace("ES async op success 1, waiting: " + actionCount.decrementAndGet());
				}

				@Override
				public void onFailure(Exception ex) {
					Throwable t = Exceptions.unwrap(ex);
					logger().warn("Elastic connection op failed [" + t + "], [" + retries.size() + "] failover, waiting: " + actionCount
							.decrementAndGet(), t);
					failing.accept(retries);
				}
			});
		} catch (IllegalStateException ex) {
			logger().error("Elastic client fail: [" + ex.getMessage() + "]");
			failing.accept(retries);
		}
		return s;
	}

	private boolean update(String type, ElasticMessage es) {
		es.table(type);
		DocWriteRequest<?> req = es.forWrite();
		if (req instanceof IndexRequest) get(conn.client().index((IndexRequest) req), 0);
		else if (req instanceof UpdateRequest) get(conn.client().update((UpdateRequest) req), 0);
		else return false;
		return true;
	}

	private final class Result {
		final long succs;
		final long fails;
		final String sample;

		Result(long succs, long fails, String sample) {
			super();
			this.succs = succs;
			this.fails = fails;
			this.sample = sample;
		}

		void trace(long total, long bytes, int retry, int remains, long ms) {
			StringBuilder sb = new StringBuilder()//
					.append("Try#").append(retry).append(" finished, total:[").append(total).append("/")//
					.append(Texts.formatKilo(bytes, " bytes")).append("] in [").append(ms)//
					.append("] ms, step successed:[" + succs + "], ");
			if (remains > 0) sb.append("remained:[").append(remains).append("], ");
			if (fails > 0) sb.append("failed:[").append(fails).append("], ");
			logger().trace(sb.append("sample id: [").append(sample).append("]."));
		}
	}

	private <T> T get(ActionFuture<T> f, int retry) {
		try {
			return f.actionGet();
		} catch (Exception e) {} finally {}
		return null;
	}

	private boolean failed(BulkItemResponse r) {
		Throwable cause = r.getFailure().getCause();
		Class<? extends Throwable> c = cause.getClass();
		if (MapperException.class.isAssignableFrom(c)) logger().error("ES mapper exception", cause);
		return EsRejectedExecutionException.class.isAssignableFrom(c) || VersionConflictEngineException.class.isAssignableFrom(c)
				|| MapperException.class.isAssignableFrom(c);
	}

	static {
		unwrap(RemoteTransportException.class, "getCause");
	}
}
