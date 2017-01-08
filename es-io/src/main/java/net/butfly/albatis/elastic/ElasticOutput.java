package net.butfly.albatis.elastic;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.update.UpdateRequest;

import net.butfly.albacore.io.Output;
import net.butfly.albacore.utils.Systems;

public class ElasticOutput extends Output<ElasticMessage> {
	private static final long serialVersionUID = 1227554461265245482L;
	private final ElasticConnect conn;

	public ElasticOutput(String name, ElasticConnect conn) throws IOException {
		super(name);
		this.conn = conn;
	}

	@Override
	public boolean enqueue0(ElasticMessage s) {
		if (s == null) return false;
		conn.getClient().update(build(s)).actionGet();
		return true;
	}

	@Override
	public long enqueue(List<ElasticMessage> docs) {
		BulkRequest req = new BulkRequest();
		for (ElasticMessage d : docs)
			if (null != d) req.add(build(d));
		long s = 0;
		for (BulkItemResponse r : conn.getClient().bulk(req).actionGet())
			if (!r.isFailed()) s++;
		return s;
	}

	private UpdateRequest build(ElasticMessage d) {
		UpdateRequest req = new UpdateRequest();
		req.docAsUpsert(d.isUpsert());
		req.index(d.getIndex());
		req.type(Systems.suffixDebug(d.getType(), logger));
		req.id(d.getId());
		req.doc(d.getValues());
		return req;
	}

	@Override
	public void closing() {
		super.closing();
		conn.close();
	}
}
