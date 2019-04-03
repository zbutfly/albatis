package net.butfly.albatis.elastic;

import static net.butfly.albacore.paral.Sdream.of;
import static net.butfly.albacore.utils.Exceptions.unwrap;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.transport.RemoteTransportException;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albacore.utils.logger.Statistic;
import net.butfly.albatis.DataConnection;
import net.butfly.albatis.io.OutputBase;
import net.butfly.albatis.io.Rmap;

public abstract class ElasticOutputBase<T extends DataConnection<?> & ElasticConnect> extends OutputBase<Rmap> {
	private static final long serialVersionUID = 1874320396863861434L;
	static final Logger logger = Logger.getLogger(ElasticOutput.class);
	public static final int MAX_RETRY = 3;
	public static final int SUGGEST_BATCH_SIZE = 1000;
	protected final T conn;

	public ElasticOutputBase(String name, T conn) throws IOException {
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
		for (Rmap m : msgs.list()) remains.put(m.key(), conn.fixTable(m));
		if (!remains.isEmpty()) go(remains);
	}

	protected abstract Function<BulkRequest, BulkResponse> request();

	protected void go(Map<Object, Rmap> remains) {
		// logger.error("INFO: es op task enter with [" + ops.get() + "] pendings.");
		int retry = 0;
		// int size = remains.size();
		try {
			while (!remains.isEmpty() && retry++ <= MAX_RETRY) {
				List<DocWriteRequest<?>> reqs = Colls.list();
				remains.values().forEach(r -> reqs.add(Elastics.forWrite(r)));
				if (reqs.isEmpty()) return;
				BulkRequest bulk = new BulkRequest().add(reqs);
				process(remains, s().statsOut(bulk, request()));
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
				if (null != o && null != r.getResponse()) logger.trace(() -> //
				"es writing successed: \n\tData: " + o.toString() + "\n\tResp: " + r.getResponse().toString());
			} else if (null != o) {
				if (noRetry(r.getFailure().getCause())) logger.error(() -> //
				"ElasticOutput [" + name() + "] failed for [" + unwrap(r.getFailure().getCause()).toString() + "]: \n\t" + o.toString());
				else retries.add(o);
			}
		}
		if (succs > 0) succeeded(succs);
		// process failing and retry...
		if (!retries.isEmpty()) failed(of(retries));
		// logger.error("INFO: es op resp parsed: resps [" + respSize + "], remains [" + originalSize + "=>" + remains.size() + "], "//
		// + "success [" + succs + "], failover [" + retries.size() + "].");
	}

	abstract boolean noRetry(Throwable cause);

	static {
		unwrap(RemoteTransportException.class, "getCause");
	}
}
