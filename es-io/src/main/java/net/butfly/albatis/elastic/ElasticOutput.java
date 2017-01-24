package net.butfly.albatis.elastic;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;

import net.butfly.albacore.io.OutputImpl;
import net.butfly.albacore.utils.Collections;

@Deprecated
public class ElasticOutput extends OutputImpl<ElasticMessage> {
	private final ElasticConnection conn;

	public ElasticOutput(ElasticConnection conn) throws IOException {
		this.conn = conn;
	}

	@Override
	public boolean enqueue(ElasticMessage s, boolean block) {
		if (s == null) return false;
		ActionFuture<UpdateResponse> f = conn.client().update(s.update());
		if (block) try {
			f.actionGet();
			return true;
		} catch (Exception e) {
			logger().error("Failure", e);
			return false;
		}
		else return true;
	}

	@Override
	public long enqueue(List<ElasticMessage> docs) {
		long s = 0;
		for (BulkItemResponse r : conn.client().bulk(new BulkRequest().add(Collections.transN(docs, d -> d.update()).toArray(
				new UpdateRequest[0]))).actionGet())
			if (!r.isFailed()) s++;
		return s;
	}

	@Override
	public void close() {
		super.close(conn::close);
	}
}
