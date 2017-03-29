package net.butfly.albatis.kudu;

import static com.hzcominfo.albatis.nosql.Connection.PARAM_KEY_BATCH;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Upsert;

import net.butfly.albacore.io.IO;
import net.butfly.albacore.io.faliover.Failover.FailoverException;
import net.butfly.albacore.io.faliover.FailoverOutput;
import net.butfly.albacore.io.utils.URISpec;

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
	protected long write(String table, Collection<KuduResult> pkg) throws FailoverException {
		List<Upsert> rows = IO.list(pkg, KuduResult::forWrite);
		rows.stream().forEach(r -> {
			try {
				session.apply(r);
			} catch (KuduException e) {
				logger().error("%s table upsert records faile.", table);
			}
		});

		return 0;
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
