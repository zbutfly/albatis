package net.butfly.albatis.kudu;

import static com.hzcominfo.albatis.nosql.Connection.PARAM_KEY_BATCH;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.Upsert;

import net.butfly.albacore.io.faliover.FailoverOutput;
import net.butfly.albacore.io.utils.URISpec;

public class KuduOutput extends FailoverOutput<String, KuduResult> {
	
	private final KuduConnection connect;
	private final KuduSession session;
	private final KuduTable kuduTable;

	public KuduOutput(String name, URISpec uri, String failoverPath) throws IOException {
		
		super(name, b -> new KuduResult(b), failoverPath,
				null == uri ? 200 : Integer.parseInt(uri.getParameter(PARAM_KEY_BATCH, "200")));
		this.connect = new KuduConnection(uri, null);
		this.session = connect.newSession();
		this.kuduTable = connect.kuduTable(name);
	}

	@Override
	protected void closeInternal() {		
		connect.close();		
	}

	@Override
	protected long write(String table, Stream<KuduResult> pkg, Set<KuduResult> fails) {
		// TODO Auto-generated method stub
		AtomicLong c = new AtomicLong(0);		
		Schema schema = kuduTable.getSchema();
		pkg.parallel().forEach(r -> {
			Map<String, Object> record = r.forWrite();
			Upsert upsert = kuduTable.newUpsert();
			schema.getColumns().forEach(p -> {
				KuduCommon.generateColumnData(p.getType(), upsert.getRow(), p.getName(), record.get(p.getName()));
			});
			try {
				OperationResponse rr = session.apply(upsert);
				if (rr.hasRowError()) {
					fails.add(r);
				} else {
					c.incrementAndGet();
				}
			} catch (KuduException ex) {
				fails.add(r);
			}

		});
		return c.get();
	}
}
