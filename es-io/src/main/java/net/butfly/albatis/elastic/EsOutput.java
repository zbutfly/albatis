package net.butfly.albatis.elastic;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.update.UpdateRequest;

import net.butfly.albacore.io.faliover.FailoverOutput;
import net.butfly.albacore.utils.Collections;
import scala.Tuple2;

public final class EsOutput extends FailoverOutput<ElasticMessage, ElasticMessage> {
	private final ElasticConnection conn;

	public EsOutput(String name, String esUri, String failoverPath) throws IOException {
		super(name, failoverPath, 100, 20);
		conn = new ElasticConnection(esUri);
		open();
	}

	public ElasticConnection getConnection() {
		return conn;
	}

	@Override
	protected void closeInternal() {
		conn.close();
	}

	@Override
	protected int write(String key, List<ElasticMessage> values) {
		// TODO: List<ElasticMessage> fails = new ArrayList<>();
		try {
			ActionFuture<BulkResponse> resps = conn.client().bulk(new BulkRequest().add(Collections.transN(values, d -> d.update()).toArray(
					new UpdateRequest[0])));
			int c = 0;
			for (BulkItemResponse r : resps.actionGet())
				if (!r.isFailed()) c++;
			return c;
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected Tuple2<String, ElasticMessage> parse(ElasticMessage e) {
		return new Tuple2<>("", e);
	}

	@Override
	protected ElasticMessage unparse(String key, ElasticMessage value) {
		return value;
	}
}
