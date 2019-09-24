package net.butfly.albatis.elastic;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.index.mapper.MapperException;
import org.elasticsearch.transport.RemoteTransportException;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static net.butfly.albacore.utils.Exceptions.unwrap;

public class ElasticOutput extends ElasticOutputBase<ElasticConnection> {

    private static final long serialVersionUID = -1305036948294851549L;

    public ElasticOutput(String name, ElasticConnection conn) throws IOException {
        super(name, conn);
    }

    @Override
    protected Function<BulkRequest, BulkResponse> request() {
        return b -> {
            try {
                return conn.client.bulk(b).get();
            } catch (ExecutionException ex) {
                logger().error("Elastic client fail: [" + ex.getCause() + "]");
            } catch (Exception ex) {
                logger().error("Elastic client fail: [" + ex + "]");
            }
            return null;
        };
    }

    @Override
    boolean noRetry(Throwable cause) {
        while (RemoteTransportException.class.isAssignableFrom(cause.getClass()) && cause.getCause() != null) cause = cause.getCause();
        if (MapperException.class.isAssignableFrom(cause.getClass())) logger().error("ES mapper exception", cause);
        return MapperException.class.isAssignableFrom(cause.getClass());
    }

    static {
        unwrap(RemoteTransportException.class, "getCause");
    }
}

