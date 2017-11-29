package net.butfly.albatis.elastic;

import static net.butfly.albacore.paral.Sdream.of;
import static net.butfly.albacore.utils.Exceptions.unwrap;

import java.io.IOException;
import java.util.List;
import java.util.Map;

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
import net.butfly.albacore.utils.Exceptions;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albacore.utils.parallel.Lambdas;
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
		Output.super.close();
	}

	@Override
	public final void enqueue(Sdream<Message> msgs) {
		List<Message> ol = msgs.list();
		if (ol.isEmpty()) return;
		Map<String, Message> remains = of(ol).partition(Message::key, conn::fixTable, Lambdas.nullOr());
		for (Message m : ol)
			remains.computeIfAbsent(m.key(), k -> conn.fixTable(m));
		Exeter.of().submit(() -> {
			int retry = 0;
			while (!remains.isEmpty() && retry++ < maxRetry) {
				@SuppressWarnings("rawtypes")
				List<DocWriteRequest> reqs = of(remains.values()).map(Elastics::forWrite).list();
				if (reqs.isEmpty()) return;
				BulkRequest req = new BulkRequest().add(reqs);
				try {
					conn.client().bulk(req, new EnqueueListener(remains, req));
				} catch (IllegalStateException ex) {
					logger().error("Elastic client fail: [" + ex.toString() + "]");
				}
			}
		});
	}

	private class EnqueueListener implements ActionListener<BulkResponse> {
		private final BulkRequest req;
		private final Map<String, Message> remains;

		private EnqueueListener(Map<String, Message> remains, BulkRequest req) {
			this.remains = remains;
			this.req = req;
		}

		@Override
		public void onResponse(BulkResponse response) {
			Map<Integer, List<BulkItemResponse>> resps = of(response).partition(r -> r.isFailed() ? //
					(noRetry(r.getFailure().getCause()) ? 1 : 2) : 0, r -> r);
			List<BulkItemResponse> succs = resps.get(0);
			List<BulkItemResponse> fails = resps.get(1);
			List<BulkItemResponse> retries = resps.get(2);

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

		}

		@Override
		public void onFailure(Exception e) {
			Throwable t = Exceptions.unwrap(e);
			if (noRetry(t)) logger().warn("Elastic connection op failed [" + t + "], [" + remains.size() + "] fails", t);
			else failed(of(remains.values()));
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
