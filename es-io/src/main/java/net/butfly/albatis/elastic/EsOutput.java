package net.butfly.albatis.elastic;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import net.butfly.albacore.io.faliover.FailoverOutput;
import net.butfly.albacore.utils.async.Concurrents;
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
			if (v.isEmpty()) return 0;
			BulkRequest req = new BulkRequest().add(v.toArray(new UpdateRequest[v.size()]));
			logger().trace("Bulk size: " + req.estimatedSizeInBytes());
			do {
				try {
					Map<Boolean, List<BulkItemResponse>> resps = StreamSupport.stream(conn.client().bulk(req).actionGet().spliterator(),
							true).collect(Collectors.partitioningBy(r -> r.isFailed()));
					if (resps.get(Boolean.TRUE).isEmpty()) return resps.get(Boolean.FALSE).size();
					else throw resps.get(Boolean.TRUE).get(0).getFailure().getCause();
				} catch (EsRejectedExecutionException e) {
					Concurrents.waitSleep(100);
				}
			} while (true);
		} catch (RuntimeException e) {
			throw e;
		} catch (Throwable e) {
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
