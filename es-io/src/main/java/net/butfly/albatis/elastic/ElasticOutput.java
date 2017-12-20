package net.butfly.albatis.elastic;

import static net.butfly.albacore.paral.Sdream.of;
import static net.butfly.albacore.utils.Exceptions.unwrap;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.mapper.MapperException;
import org.elasticsearch.transport.RemoteTransportException;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.paral.Task;
import net.butfly.albacore.utils.Exceptions;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albacore.utils.parallel.Lambdas;
import net.butfly.albatis.io.Message;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.SafeOutput;

public final class ElasticOutput extends SafeOutput<Message> {
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
		super.close();
		try {
			conn.close();
		} catch (Exception e) {}
	}

	@Override
	protected final void enqueue(Sdream<Message> msgs, AtomicInteger ops) {
		ops.incrementAndGet();
		try {
			List<Message> ol = msgs.list();
			if (ol.isEmpty()) return;
			Map<String, Message> remains = of(ol).partition(Message::key, conn::fixTable, Lambdas.nullOr());
			// Exeter.of().submit(() -> {
			int retry = 0;
			while (!remains.isEmpty() && retry++ < maxRetry) {
				@SuppressWarnings("rawtypes")
				List<DocWriteRequest> reqs = of(remains.values()).map(Elastics::forWrite).list();
				if (reqs.isEmpty()) return;
				BulkRequest req = new BulkRequest().add(reqs);
				EnqueueListener lis = new EnqueueListener(remains, req);
				try {
					conn.client().bulk(req, lis);
					lis.join();
				} catch (IllegalStateException ex) {
					logger().error("Elastic client fail: [" + ex.toString() + "]");
				}
			}
			// });
		} finally {
			ops.decrementAndGet();
		}
	}

	private class EnqueueListener implements ActionListener<BulkResponse> {
		private final BulkRequest req;
		private final Map<String, Message> remains;
		private final AtomicBoolean finished = new AtomicBoolean(false);

		private EnqueueListener(Map<String, Message> remains, BulkRequest req) {
			this.remains = remains;
			this.req = req;
		}

		public void join() {
			while (!finished.get())
				Task.waitSleep(10);
		}

		@Override
		public void onResponse(BulkResponse response) {
			List<BulkItemResponse> succs = Colls.list();
			List<BulkItemResponse> fails = Colls.list();
			List<BulkItemResponse> retries = Colls.list();
			for (BulkItemResponse r : response)
				if (!r.isFailed()) succs.add(r);
				else if (noRetry(r.getFailure().getCause())) fails.add(r);
				else retries.add(r);
			if (!succs.isEmpty()) {
				logger.trace(() -> "Elastic enqueue succeed [" + req.estimatedSizeInBytes() + " bytes], sample id: " + succs.get(0)
						.getId());
				succeeded(succs.size());
			}
			// process success: remove from remains
			succs.forEach(succ -> remains.remove(succ.getId()));
			// process failing and retry...
			if (!retries.isEmpty()) failed(of(retries).map(r -> remains.remove(r.getId())));
			if (!fails.isEmpty() && logger().isTraceEnabled()) {
				logger.warn(() -> of(fails).joinAsString("Some fails: \n", //
						r -> "\tfailed id [" + r.getFailure().getId() + "] for: " + unwrap(r.getFailure().getCause()).toString(), "\n"));
			}
			finished.set(true);
		}

		@Override
		public void onFailure(Exception e) {
			Throwable t = Exceptions.unwrap(e);
			if (noRetry(t)) logger().warn("Elastic connection op failed [" + t + "], [" + remains.size() + "] fails", t);
			else failed(of(remains.values()));
			finished.set(true);
		}

		private boolean noRetry(Throwable cause) {
			while (RemoteTransportException.class.isAssignableFrom(cause.getClass()) && cause.getCause() != null)
				cause = cause.getCause();
			if (MapperException.class.isAssignableFrom(cause.getClass())) logger().error("ES mapper exception", cause);
			return EsRejectedExecutionException.class.isAssignableFrom(cause.getClass())//
					// ||
					// VersionConflictEngineException.class.isAssignableFrom(c)
					|| MapperException.class.isAssignableFrom(cause.getClass());
		}
	}

	static {
		unwrap(RemoteTransportException.class, "getCause");
	}
}
