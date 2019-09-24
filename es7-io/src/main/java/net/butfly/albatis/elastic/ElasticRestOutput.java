package net.butfly.albatis.elastic;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.mapper.MapperException;

import java.io.IOException;
import java.util.function.Function;

import static net.butfly.albacore.utils.Exceptions.unwrap;

public class ElasticRestOutput extends ElasticOutputBase<ElasticRestHighLevelConnection> {
	private static final long serialVersionUID = 3882692296354935800L;

	protected ElasticRestOutput(String name, ElasticRestHighLevelConnection conn) throws IOException {
		super(name, conn);
	}

	@Override
	protected Function<BulkRequest, BulkResponse> request() {
		return b -> {
			try {
				return conn.client.bulk(b, RequestOptions.DEFAULT);
			} catch (IOException ex) {
				logger().error("Elastic client fail: [" + ex + "]");
			}
			return null;
		};
	}

	@Override
	boolean noRetry(Throwable cause) {
		while (ElasticsearchException.class.isAssignableFrom(cause.getClass()) && cause.getCause() != null)
			cause = cause.getCause();
		if (MapperException.class.isAssignableFrom(cause.getClass())) logger().error("ES mapper exception", cause);
		return MapperException.class.isAssignableFrom(cause.getClass());
	}

	static {
		unwrap(ElasticsearchException.class, "getCause");
	}

}
