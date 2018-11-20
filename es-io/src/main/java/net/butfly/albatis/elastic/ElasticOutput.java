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

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albacore.utils.logger.Statistic;
import net.butfly.albatis.io.OutputBase;
import net.butfly.albatis.io.Rmap;

public class ElasticOutput extends OutputBase<Rmap> {
	private static final long serialVersionUID = 1874320396863861434L;
	static final Logger logger = Logger.getLogger(ElasticOutput.class);
	public static final int MAX_RETRY = 3;
	public static final int SUGGEST_BATCH_SIZE = 1000;
	protected final ElasticConnection conn;

	public ElasticOutput(String name, ElasticConnection conn) throws IOException {
		super(name);
		this.conn = conn;
	}

	@Override
	public URISpec target() {
		return conn.uri();
	}

	@Override
	public Statistic trace() {
		return new Statistic(this).sizing(BulkRequest::estimatedSizeInBytes)//
				.<BulkRequest> batchSizeCalcing(r -> (long) r.requests().size())//
				.<BulkRequest> sampling(r -> r.requests().isEmpty() ? null : r.requests().get(0).toString());
	}

	@Override
	public void close() {
		super.close();
		try {
			conn.close();
		} catch (Exception e) {}
	}

	@Override
	protected void enqsafe(Sdream<Rmap> msgs) {
		Map<Object, Rmap> remains = Maps.of();
		for (Rmap m : msgs.list())
			remains.put(m.key(), conn.fixTable(m));
		if (!remains.isEmpty()) go(remains);
	}

	protected void go(Map<Object, Rmap> remains) {
		// logger.error("INFO: es op task enter with [" + ops.get() + "] pendings.");
		int retry = 0;
		// int size = remains.size();
		try {
			while (!remains.isEmpty() && retry++ <= MAX_RETRY) {
				@SuppressWarnings("rawtypes")
				List<DocWriteRequest> reqs = of(remains.values()).map(Elastics::forWrite).list();
				if (reqs.isEmpty()) return;
				BulkRequest bulk = new BulkRequest().add(reqs);
				process(remains, s().statsOut(bulk, b -> {
					// logger().error("Elastic step [" + size + "] begin.");
					// long now = System.currentTimeMillis();
					try {
						return conn.client.bulk(b).get();
					} catch (ExecutionException ex) {
						logger().error("Elastic client fail: [" + ex.getCause() + "]");
					} catch (Exception ex) {
						logger().error("Elastic client fail: [" + ex + "]");
						// } finally {
						// logger().error("Elastic step [" + size + "] spent: " + (System.currentTimeMillis() - now) + " ms.");
					}
					return null;
				}));
			}
		} finally {
			if (!remains.isEmpty()) failed(of(remains.values()));
		}
	}

	void process(Map<Object, Rmap> remains, BulkResponse response) {
		int succs = 0, respSize = remains.size();
		// int originalSize = remains.size();
		if (respSize == 0) return;
		List<Rmap> retries = Colls.list();
		if (null != response) for (BulkItemResponse r : response) {
			Rmap o = remains.remove(r.getId());
			if (!r.isFailed()) {
				succs++;
				logger.trace(() -> "es writing successed: \n\tData: " + o.toString() + "\n\tResp: " + r.getResponse().toString());
			} else if (null != o) {
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
