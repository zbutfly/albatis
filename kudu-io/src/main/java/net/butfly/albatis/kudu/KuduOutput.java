package net.butfly.albatis.kudu;

import static com.hzcominfo.albatis.nosql.Connection.PARAM_KEY_BATCH;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.SessionConfiguration.FlushMode;
import org.apache.kudu.client.Upsert;

import net.butfly.albacore.io.faliover.FailoverOutput;
import net.butfly.albacore.io.utils.URISpec;

public class KuduOutput extends FailoverOutput<String, KuduResult> {

	private final KuduConnection connect;
	private final KuduSession session;
	private KuduTable kuduTable;

	public KuduOutput(String name, URISpec uri, String failoverPath) throws IOException {

		super(name, b -> new KuduResult(b), failoverPath,
				null == uri ? 200 : Integer.parseInt(uri.getParameter(PARAM_KEY_BATCH, "200")));
		this.connect = new KuduConnection(uri, null);
		this.session = connect.newSession();
		this.session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND);
		this.session.setTimeoutMillis(10000);
		open();
		failHandler();
	}

	@Override
	protected void closeInternal() {
		try {
			session.flush();
		} catch (KuduException e) {
			// TODO Auto-generated catch block
			throw new RuntimeException("kudu flush faile.");
		}finally {
			connect.close();
		}
	}

	@Override
	protected long write(String table, Stream<KuduResult> pkg, Consumer<Collection<KuduResult>> failing,
			Consumer<Long> committing, int retry) {
		try {
			this.kuduTable = connect.kuduTable(name);
		} catch (KuduException e) {
			// TODO Auto-generated catch block
			throw new RuntimeException("connect kudu table faile."+e.getMessage());
		}
		AtomicLong c = new AtomicLong(0);
		Schema schema = kuduTable.getSchema();
		List<String> keys = new ArrayList<>();
		schema.getPrimaryKeyColumns().forEach(p->{
			keys.add(p.getName());
		});
		pkg.parallel().forEach(r -> {
			Map<String, Object> record = r.forWrite();
			if(record.keySet().containsAll(keys)){
				Upsert upsert = kuduTable.newUpsert();
				schema.getColumns().forEach(p -> {
					if (record.containsKey(p.getName()))
						KuduCommon.generateColumnData(p.getType(), upsert.getRow(), p.getName(), record.get(p.getName()));
				});
				try {
					session.apply(upsert);
					c.incrementAndGet();
				} catch (KuduException ex) {
					failing.accept(Arrays.asList(r));
				}
			}			
		});
		return c.get();
	}

	public KuduClient client() {
		return connect.client();
	}

	public void failHandler() {
		new Thread(new Runnable() {
			@Override
			public void run() {
				// TODO Auto-generated method stub
				for (;;) {
					if (session.getPendingErrors() != null) {
						Arrays.asList(session.getPendingErrors().getRowErrors()).forEach(p -> {
							try {
								session.apply(p.getOperation());
							} catch (KuduException e) {
								// TODO Auto-generated catch block
								throw new RuntimeException("retry apply operation fail.");
							}
						});
					}else{
						try {
							TimeUnit.SECONDS.sleep(1);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							throw new RuntimeException("retry time waitting exception.");
						}
					}
				}
			}
		}).start();

	}
}
