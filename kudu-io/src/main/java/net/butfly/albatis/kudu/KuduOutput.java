package net.butfly.albatis.kudu;

import static com.hzcominfo.albatis.nosql.Connection.PARAM_KEY_BATCH;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.kudu.client.KuduClient;

import net.butfly.albacore.io.faliover.FailoverOutput;
import net.butfly.albacore.io.utils.URISpec;

public class KuduOutput extends FailoverOutput<String, KuduResult> {
	private final KuduConnection connect;

	public KuduOutput(String name, URISpec uri, String failoverPath) throws IOException {
		super(name, b -> new KuduResult(b), failoverPath, null == uri ? 200 : Integer.parseInt(uri.getParameter(PARAM_KEY_BATCH, "200")));
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
	protected long write(String table, Stream<KuduResult> pkg, Consumer<Collection<KuduResult>> failing, Consumer<Long> committing,
			int retry) {
		AtomicLong c = new AtomicLong(0);
		pkg.forEach(r -> {
			if (connect.upsert(table, r.forWrite())) c.incrementAndGet();
			else failing.accept(Arrays.asList(r));
		});
		return c.get();
	}

	@Deprecated
	public KuduClient client() {
		return connect.client();
	}
}
