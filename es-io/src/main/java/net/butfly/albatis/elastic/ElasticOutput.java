package net.butfly.albatis.elastic;

import static net.butfly.albacore.utils.Exceptions.unwrap;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.index.mapper.MapperException;
import org.elasticsearch.transport.RemoteTransportException;

import net.butfly.albacore.utils.logger.Logger;

public class ElasticOutput extends ElasticOutputBase<ElasticConnection> {
	private static final long serialVersionUID = 1874320396863861434L;
	static final Logger logger = Logger.getLogger(ElasticOutput.class);
	public static final int MAX_RETRY = 3;
	public static final int SUGGEST_BATCH_SIZE = 1000;

	public ElasticOutput(String name, ElasticConnection conn) throws IOException {
		super(name, conn);
	}

	@Override
	protected Function<BulkRequest, BulkResponse> request() {
		return b -> {
			// logger().error("Elastic step [" + size + "] begin.");
			// long now = System.currentTimeMillis();
			try {
				return conn.client.bulk(b).get();
			} catch (ExecutionException ex) {
				logger().error("Elastic client fail: [" + ex.getCause() + "]");
			} catch (Exception ex) {
				logger().error("Elastic client fail: [" + ex + "]");
				// } finally {
				// logger().error("Elastic step [" + size + "] spent: " + (System.currentTimeMillis() - now) + " ms.");
			}
			return null;
		};
	}

	@Override
	boolean noRetry(Throwable cause) {
		while (RemoteTransportException.class.isAssignableFrom(cause.getClass()) && cause.getCause() != null) cause = cause.getCause();
		if (MapperException.class.isAssignableFrom(cause.getClass())) logger().error("ES mapper exception", cause);
		return // EsRejectedExecutionException.class.isAssignableFrom(cause.getClass()) ||
				// VersionConflictEngineException.class.isAssignableFrom(c) ||
		MapperException.class.isAssignableFrom(cause.getClass());
	}

	static {
		unwrap(RemoteTransportException.class, "getCause");
	}
}
