package net.butfly.albatis.elastic;

import java.io.IOException;
import java.util.Iterator;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;

import net.butfly.albacore.io.OutputQueue;
import net.butfly.albacore.io.OutputQueueImpl;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.Systems;

public class ElasticOutput<T> extends OutputQueueImpl<T, ElasticOutputDoc> implements OutputQueue<T> {
	private static final long serialVersionUID = 1227554461265245482L;
	protected final TransportClient elastic;

	public ElasticOutput(final String cluster, final String hostports, Converter<T, ElasticOutputDoc> conv) throws IOException {
		this(cluster, hostports, conv, 1);
	}

	public ElasticOutput(final String cluster, final String hostports, Converter<T, ElasticOutputDoc> conv, final int parallelism)
			throws IOException {
		this(cluster, hostports, conv, parallelism, 4096);
	}

	public ElasticOutput(final String cluster, final String hostports, Converter<T, ElasticOutputDoc> conv, final int parallelism,
			final int outputBatchBytes) throws IOException {
		super("elastic-output-queue", conv);
		elastic = Elastics.connect(cluster, hostports.split(","));
	}

	@Override
	protected boolean enqueueRaw(T s) {
		UpdateRequest req = build(conv.apply(s));
		if (null == req) return false;
		UpdateResponse r = elastic.update(req).actionGet();
		return true;
	}

	@Override
	public long enqueue(Iterator<T> iter) {
		BulkRequest req = new BulkRequest();
		while (iter.hasNext()) {
			UpdateRequest r = build(conv.apply(iter.next()));
			if (null != r) req.add(r);
		}
		long s = 0;
		for (BulkItemResponse r : elastic.bulk(req).actionGet())
			if (!r.isFailed()) s++;
		return s;
	}

	private UpdateRequest build(ElasticOutputDoc d) {
		if (d == null) return null;
		UpdateRequest req = new UpdateRequest();
		req.docAsUpsert(d.isUpsert());
		req.index(d.getIndex());
		req.type(Systems.suffixDebug(d.getType(), logger));
		req.id(d.getId());
		req.doc(d.getValues());
		return req;
	}
}
