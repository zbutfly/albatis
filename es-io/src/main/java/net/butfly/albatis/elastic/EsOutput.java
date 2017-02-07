package net.butfly.albatis.elastic;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.update.UpdateRequest;

import net.butfly.albacore.io.faliover.FailoverOutput;
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
	protected int write(String key, Collection<ElasticMessage> values) {
		// TODO: List<ElasticMessage> fails = new ArrayList<>();
		try {
			List<UpdateRequest> v = values.parallelStream().filter(t -> null != t).map(ElasticMessage::update).collect(Collectors.toList());
			BulkRequest req = new BulkRequest().add(v.toArray(new UpdateRequest[v.size()]));
			logger().trace("Bulk size: " + req.estimatedSizeInBytes());
			ActionFuture<BulkResponse> resp = conn.client().bulk(req);
			int c = 0;
			for (BulkItemResponse r : resp.actionGet())
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
