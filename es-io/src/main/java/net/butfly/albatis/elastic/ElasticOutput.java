package net.butfly.albatis.elastic;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.update.UpdateResponse;

import net.butfly.albacore.io.OutputImpl;

@Deprecated
public final class ElasticOutput extends OutputImpl<ElasticMessage> {
	private final ElasticConnection conn;

	public ElasticOutput(ElasticConnection conn) throws IOException {
		this.conn = conn;
		open();
	}

	@Override
	public boolean enqueue(ElasticMessage s) {
		if (s == null) return false;
		ActionFuture<UpdateResponse> f = conn.client().update(s.update());
		try {
			f.actionGet();
			return true;
		} catch (Exception e) {
			logger().error("Failure", e);
			return false;
		}
	}

	@Override
	public long enqueue(Stream<ElasticMessage> docs) {
		long s = 0;
		for (BulkItemResponse r : conn.client().bulk(new BulkRequest().add(docs.filter(t -> t != null).map(ElasticMessage::update).collect(
				Collectors.toList()))).actionGet())
			if (!r.isFailed()) s++;
		return s;
	}

	@Override
	public void close() {
		super.close(conn::close);
	}
}
