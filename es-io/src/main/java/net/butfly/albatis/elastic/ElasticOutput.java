package net.butfly.albatis.elastic;

import java.io.IOException;
import java.util.Iterator;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;

import net.butfly.albacore.io.OutputImpl;
import net.butfly.albacore.utils.Systems;

public class ElasticOutput extends OutputImpl<ElasticOutputDoc> {
	private static final long serialVersionUID = 1227554461265245482L;
	protected final TransportClient elastic;

	public ElasticOutput(final String cluster, final String hostports) throws IOException {
		this(cluster, hostports, 1);
	}

	public ElasticOutput(final String cluster, final String hostports, final int parallelism) throws IOException {
		this(cluster, hostports, parallelism, 4096);
	}

	public ElasticOutput(final String cluster, final String hostports, final int parallelism, final int outputBatchBytes)
			throws IOException {
		super("elastic-output-queue");
		elastic = Elastics.connect(cluster, hostports.split(","));
	}

	@Override
	public boolean enqueue(ElasticOutputDoc s) {
		UpdateRequest req = build(s);
		if (null == req) return false;
		elastic.update(req).actionGet();
		return true;
	}

	@Override
	public long enqueue(Iterator<ElasticOutputDoc> iter) {
		BulkRequest req = new BulkRequest();
		while (iter.hasNext()) {
			UpdateRequest r = build(iter.next());
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
