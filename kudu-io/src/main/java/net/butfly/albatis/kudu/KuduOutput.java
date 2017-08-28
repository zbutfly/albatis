package net.butfly.albatis.kudu;

import static com.hzcominfo.albatis.nosql.Connection.PARAM_KEY_BATCH;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.kudu.client.KuduClient;

import net.butfly.albacore.io.Record;
import net.butfly.albacore.io.faliover.FailoverOutput;
import net.butfly.albacore.io.utils.URISpec;

public class KuduOutput extends FailoverOutput {
	public static final long DEFAULT_BATCH_SIZE = 200;
	private final KuduConnection connect;

	public KuduOutput(String name, URISpec uri, String failoverPath) throws IOException {
		super(name, b -> Record.fromBytes(b), failoverPath, null == uri ? 0 : Integer.parseInt(uri.getParameter(PARAM_KEY_BATCH, "0")));
		connect = new KuduConnection(uri, null);
		open();
	}

	public KuduOutput(String name, URISpec uri, String failoverPath, long batchSize) throws IOException {
		super(name, b -> Record.fromBytes(b), failoverPath, (int) (batchSize > 0 ? batchSize : DEFAULT_BATCH_SIZE));
		connect = new KuduConnection(uri, null);
		open();
	}

	@Override
	protected void closeInternal() {
		commit(null);
		connect.close();
	}

	@Override
	protected void commit(String key) {
		connect.commit();
	}

	@Override
	protected long write(String table, Stream<? extends Record> pkg, Consumer<Collection<? extends Record>> failing,
			Consumer<Long> committing, int retry) {
		AtomicLong c = new AtomicLong(0);
		pkg.parallel().filter(r -> {
			return r != null && !r.isEmpty();
		}).forEach(r -> {
			if (connect.upsert(table, r)) c.incrementAndGet();
			else failing.accept(Arrays.asList(r));
		});
		return c.get();
	}

	@Deprecated
	public KuduClient client() {
		return connect.client();
	}
}
