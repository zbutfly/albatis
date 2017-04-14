package net.butfly.albatis.elastic;

import java.io.IOException;
import java.util.stream.Stream;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.io.Output;
import net.butfly.albacore.io.utils.Parals;
import net.butfly.albacore.io.utils.Streams;

@Deprecated
public final class ElasticOutput extends Namedly implements Output<ElasticMessage> {
	private final ElasticConnection conn;

	public ElasticOutput(ElasticConnection conn) throws IOException {
		this.conn = conn;
		closing(conn::close);
		open();
	}

	@Override
	public long enqueue(Stream<ElasticMessage> docs) {
		long s = 0;
		for (BulkItemResponse r : conn.client().bulk(new BulkRequest().add(Parals.list(Streams.of(docs).map(ElasticMessage::forWrite))))
				.actionGet())
			if (!r.isFailed()) s++;
		return s;
	}
}
