package net.butfly.albatis.elastic;

import java.io.IOException;
import java.util.stream.Stream;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.update.UpdateResponse;

import net.butfly.albacore.io.IO;
import net.butfly.albacore.io.OutputImpl;
import net.butfly.albacore.io.Streams;

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
		ActionFuture<UpdateResponse> f = conn.client().update(s.forWrite());
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
		for (BulkItemResponse r : conn.client().bulk(new BulkRequest().add(IO.list(Streams.of(docs).map(ElasticMessage::forWrite))))
				.actionGet())
			if (!r.isFailed()) s++;
		return s;
	}

	@Override
	public void close() {
		super.close(conn::close);
	}
}
