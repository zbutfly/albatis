package net.butfly.albatis.elastic;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.update.UpdateRequest;

import net.butfly.albacore.io.Output;
import net.butfly.albacore.utils.Collections;

@Deprecated
public class ElasticOutput extends Output<ElasticMessage> {
	private static final long serialVersionUID = 1227554461265245482L;
	private final ElasticConnection conn;

	public ElasticOutput(String name, ElasticConnection conn) throws IOException {
		super(name);
		this.conn = conn;
	}

	@Override
	public boolean enqueue0(ElasticMessage s) {
		if (s == null) return false;
		conn.client().update(s.update()).actionGet();
		return true;
	}

	@Override
	public long enqueue(List<ElasticMessage> docs) {
		long s = 0;
		for (BulkItemResponse r : conn.client().bulk(new BulkRequest().add(Collections.transform(docs, d -> d.update()).toArray(
				new UpdateRequest[0]))).actionGet())
			if (!r.isFailed()) s++;
		return s;
	}

	@Override
	public void close() {
		super.close();
		conn.close();
	}
}
