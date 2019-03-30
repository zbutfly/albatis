package net.butfly.albatis.elastic;

import static net.butfly.albacore.utils.Exceptions.unwrap;

import java.io.IOException;
import java.util.function.Function;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.mapper.MapperException;

import net.butfly.albacore.utils.logger.Logger;

public class ElasticRestOutput extends ElasticOutputBase<ElasticRestHighLevelConnection> {
	private static final long serialVersionUID = 3882692296354935800L;
	
	static final Logger logger = Logger.getLogger(ElasticRestOutput.class);
	public static final int MAX_RETRY = 3;
	public static final int SUGGEST_BATCH_SIZE = 1000;

	protected ElasticRestOutput(String name, ElasticRestHighLevelConnection conn) throws IOException {
		super(name, conn);
	}

	@Override
	protected Function<BulkRequest, BulkResponse> request() {
		return b -> {
			// logger().error("Elastic step [" + size + "] begin.");
			// long now = System.currentTimeMillis();
			try {
				return conn.client.bulk(b, RequestOptions.DEFAULT);
			} catch (IOException ex) {
				logger().error("Elastic client fail: [" + ex + "]");
				// } finally {
				// logger().error("Elastic step [" + size + "] spent: " + (System.currentTimeMillis() - now) + " ms.");
			}
			return null;
		};
	}

	@Override
	boolean noRetry(Throwable cause) {
		while (ElasticsearchException.class.isAssignableFrom(cause.getClass()) && cause.getCause() != null)
			cause = cause.getCause();
		if (MapperException.class.isAssignableFrom(cause.getClass())) logger().error("ES mapper exception", cause);
		return // EsRejectedExecutionException.class.isAssignableFrom(cause.getClass()) ||
				// VersionConflictEngineException.class.isAssignableFrom(c) ||
		MapperException.class.isAssignableFrom(cause.getClass());
	}

	static {
		unwrap(ElasticsearchException.class, "getCause");
	}

}
