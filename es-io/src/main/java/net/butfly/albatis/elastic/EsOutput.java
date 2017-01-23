package net.butfly.albatis.elastic;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.update.UpdateRequest;

import net.butfly.albacore.io.faliover.FailoverOutput;
import net.butfly.albacore.utils.Collections;
import scala.Tuple2;

public class EsOutput extends FailoverOutput<ElasticMessage, ElasticMessage> {
	private static final long serialVersionUID = 1496163556789600293L;
	private final ElasticConnection conn;

	public EsOutput(String name, String esUri, String failoverPath) throws IOException {
		super(name, failoverPath, 100, 20);
		conn = new ElasticConnection(esUri);
	}

	public ElasticConnection getConnection() {
		return conn;
	}

	@Override
	protected void closeInternal() {
		conn.close();
	}

	@Override
	protected Exception write(String key, List<ElasticMessage> values) {
		// TODO: List<ElasticMessage> fails = new ArrayList<>();
		for (BulkItemResponse r : conn.client().bulk(//
				new BulkRequest().add(Collections.transN(values, d -> d.update()).toArray(new UpdateRequest[0]))//
		).actionGet())
			if (r.isFailed()) return new RuntimeException("ES bulk update failure: " + r.getFailureMessage());
		return null;
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
