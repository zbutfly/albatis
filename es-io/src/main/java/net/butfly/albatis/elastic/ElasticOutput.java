package net.butfly.albatis.elastic;

import static net.butfly.albacore.paral.Sdream.of;
import static net.butfly.albacore.utils.Exceptions.unwrap;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.index.mapper.MapperException;
import org.elasticsearch.transport.RemoteTransportException;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.io.Message;
import net.butfly.albatis.io.SafeOutput;

public class ElasticOutput extends SafeOutput<Message> {
	static final Logger logger = Logger.getLogger(ElasticOutput.class);
	public static final int MAX_RETRY = 3;
	public static final int SUGGEST_BATCH_SIZE = 1000;
	protected final ElasticConnection conn;

	public ElasticOutput(String name, ElasticConnection conn) throws IOException {
		super(name);
		this.conn = conn;
		trace(BulkRequest.class).sizing(r -> r.estimatedSizeInBytes()).stepping(r -> (long) r.requests().size())//
				.step(Long.parseLong(Configs.gets(ElasticProps.OUTPUT_STATS_STEP, "-1")));
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
	protected void enqSafe(Sdream<Message> msgs) {
		Map<String, Message> remains = Maps.of();
		for (Message m : msgs.list())
			remains.put(m.key(), conn.fixTable(m));
		if (remains.isEmpty()) {
			opsPending.decrementAndGet();
			return;
		}
		go(remains);
	}

	protected void go(Map<String, Message> remains) {
		// logger.error("INFO: es op task enter with [" + ops.get() + "] pendings.");
		int retry = 0;
		int size = remains.size();
		long now = System.currentTimeMillis();
		try {
			while (!remains.isEmpty() && retry++ <= MAX_RETRY) {
				@SuppressWarnings("rawtypes")
				List<DocWriteRequest> reqs = of(remains.values()).map(Elastics::forWrite).list();
				if (reqs.isEmpty()) return;
				try {
					process(remains, conn.client().bulk(stats(new BulkRequest().add(reqs))).get());
				} catch (ExecutionException ex) {
					logger().error("Elastic client fail: [" + ex.getCause() + "]");
				} catch (Exception ex) {
					logger().error("Elastic client fail: [" + ex + "]");
				}
			}
		} finally {
			if (!remains.isEmpty()) failed(of(remains.values()));
			opsPending.decrementAndGet();
			int ret = retry;
			logger.debug(() -> "ElasticOutput ops [" + size + "] finished in [" + (System.currentTimeMillis() - now) + " ms], remain ["
					+ remains.size() + "] after [" + ret + "] retries, pendins [" + opsPending.get() + "]");
		}
	}

	void process(Map<String, Message> remains, BulkResponse response) {
		int succs = 0, respSize = remains.size();
		// int originalSize = remains.size();
		if (respSize == 0) return;
		List<Message> retries = Colls.list();
		for (BulkItemResponse r : response) {
			Message o = remains.remove(r.getId());
			if (!r.isFailed()) succs++;
			else if (null != o) {
				if (noRetry(r.getFailure().getCause())) {
					logger.error(() -> "ElasticOutput [" + name() + "] failed for [" + unwrap(r.getFailure().getCause()).toString()
							+ "]: \n\t" + o.toString());
				} else retries.add(o);
			}

		}
		if (succs > 0) succeeded(succs);
		// process failing and retry...
		if (!retries.isEmpty()) failed(of(retries));
		// logger.error("INFO: es op resp parsed: resps [" + respSize + "], remains [" + originalSize + "=>" + remains.size() + "], "//
		// + "success [" + succs + "], failover [" + retries.size() + "].");
	}

	boolean noRetry(Throwable cause) {
		while (RemoteTransportException.class.isAssignableFrom(cause.getClass()) && cause.getCause() != null)
			cause = cause.getCause();
		if (MapperException.class.isAssignableFrom(cause.getClass())) logger().error("ES mapper exception", cause);
		return // EsRejectedExecutionException.class.isAssignableFrom(cause.getClass()) ||
				// VersionConflictEngineException.class.isAssignableFrom(c) ||
		MapperException.class.isAssignableFrom(cause.getClass());
	}

	static {
		unwrap(RemoteTransportException.class, "getCause");
	}
}
