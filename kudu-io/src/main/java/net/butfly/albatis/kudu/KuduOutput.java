package net.butfly.albatis.kudu;

import static com.hzcominfo.albatis.nosql.Connection.PARAM_KEY_BATCH;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;


import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Upsert;

import net.butfly.albacore.io.faliover.Failover.FailoverException;
import net.butfly.albacore.io.faliover.FailoverOutput;
import net.butfly.albacore.io.utils.URISpec;

/**
 * @Author Naturn
 *
 * @Date 2017年3月16日-下午8:18:39
 *
 * @Version 1.0.0
 *
 * @Email juddersky@gmail.com
 */

public class KuduOutput extends FailoverOutput<String, KuduResult> {

	private final KuduConnection connect;

	private final Map<String, KuduTable> tables;

	private final KuduSession session;

	public KuduOutput(String name, URISpec uri, String failoverPath) throws IOException {
		super(name, b -> new KuduResult(b), failoverPath,
				null == uri ? 200 : Integer.parseInt(uri.getParameter(PARAM_KEY_BATCH, "200")));
		// TODO Auto-generated constructor stub
		this.connect = new KuduConnection(uri, null);
		this.tables = new ConcurrentHashMap<>();
		this.session = connect.client().newSession();

	}

	@Override
	protected long write(String table, Stream<KuduResult> pkg) throws FailoverException {
		// TODO Auto-generated method stub
		List<Upsert> rows = pkg.map(KuduResult::forWrite).collect(Collectors.toList());
		rows.stream().forEach(r -> {
			try {
				session.apply(r);
			} catch (KuduException e) {
				// TODO Auto-generated catch block
				logger().error("%s table upsert records faile.", table);
			}
		});
		
		return 0;
	}

	@Override
	protected void closeInternal() {
		// TODO Auto-generated method stub
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

//	private KuduTable table(String table) {
//		return tables.computeIfAbsent(table, t -> {
//			try {
//				return connect.client().openTable(t);
//			} catch (IOException e) {
//				throw new RuntimeException(e);
//			}
//		});
//	}

}
