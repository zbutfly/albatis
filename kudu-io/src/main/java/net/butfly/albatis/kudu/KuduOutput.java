package net.butfly.albatis.kudu;

import static com.hzcominfo.albatis.nosql.Connection.PARAM_KEY_BATCH;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.OperationResponse;

import net.butfly.albacore.io.faliover.FailoverOutput;
import net.butfly.albacore.io.utils.URISpec;
import net.butfly.albacore.utils.Exceptions;

public class KuduOutput extends FailoverOutput<String, KuduResult> {
	private final KuduConnection connect;
	private final Map<String, KuduTable> tables;
	private final KuduSession session;

	public KuduOutput(String name, URISpec uri, String failoverPath) throws IOException {
		super(name, b -> new KuduResult(b), failoverPath, null == uri ? 200 : Integer.parseInt(uri.getParameter(PARAM_KEY_BATCH, "200")));
		this.connect = new KuduConnection(uri, null);
		this.tables = new ConcurrentHashMap<>();
		this.session = connect.client().newSession();

	}

	@Override
	protected long write(String table, Stream<KuduResult> pkg, Set<KuduResult> fails) {
		AtomicLong c = new AtomicLong();
		pkg.parallel().forEach(r -> {
			try {
				OperationResponse rr = session.apply(r.forWrite());
				logger().warn("Write fail: " + rr.getRowError().toString());
				if (rr.hasRowError()) fails.add(r);
				else c.incrementAndGet();
			} catch (KuduException ex) {
				logger().warn(name() + " write failed [" + Exceptions.unwrap(ex).getMessage() + "], \n\t[" + r.toString()
						+ "] into failover.");
				fails.add(r);
			}
		});
		return c.get();
	}

	@Override
	protected void closeInternal() {
		for (String k : tables.keySet()) {
			tables.remove(k);
			synchronized (connect) {
				try {
					connect.client().close();
				} catch (KuduException e) {
					logger().error("%s Kudu client close faile,no effect on the system.", k);
				}
			}
		}
	}
}
