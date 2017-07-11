package net.butfly.albatis.kudu;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.Delete;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.SessionConfiguration.FlushMode;
import org.apache.kudu.client.Upsert;

import com.hzcominfo.albatis.nosql.Connection;
import com.hzcominfo.albatis.nosql.NoSqlConnection;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albacore.utils.parallel.Concurrents;
import net.butfly.albatis.io.Message;

public class KuduConnection extends NoSqlConnection<KuduClient> {
	private static final Logger logger = Logger.getLogger(KuduConnection.class);
	private final KuduSession session;
	private Thread failHandler;

	static {
		Connection.register("kudu", KuduConnection.class);
	}

	public KuduConnection(URISpec kuduUri, Map<String, String> props) throws IOException {
		super(kuduUri, r -> new KuduClient.KuduClientBuilder(kuduUri.getHost()).build(), "kudu", "kudu");
		session = client().newSession();
		session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND);
		session.setTimeoutMillis(10000);
		failHandler = new Thread(() -> {
			do {
				processError();
			} while (Concurrents.waitSleep());
		} , "KuduErrorHandler[" + kuduUri.toString() + "]");
		failHandler.setDaemon(true);
		failHandler.start();
	}

	private void processError() {
		if (session.getPendingErrors() != null)
			Arrays.asList(session.getPendingErrors().getRowErrors()).forEach(p -> {
				try {
					session.apply(p.getOperation());
				} catch (KuduException e) {
					throw new RuntimeException("retry apply operation fail.");
				}
			});
	}

	@Override
	public void close() {
		try {
			super.close();
			client().close();
		} catch (IOException e) {
			logger.error("Close failure", e);
		}
	}

	public void commit() {
		try {
			session.flush();
		} catch (KuduException e) {
			logger().error("Kudu flush fail", e);
		}
	}

	public KuduTable kuduTable(String table) throws KuduException {
		return client().openTable(table);
	}

	private static final Map<String, KuduTable> tables = new ConcurrentHashMap<>();

	public KuduTable table(String table) {

		return tables.compute(table, (n, t) -> {
			if (null == t) try {
				t = client().openTable(table);
			} catch (KuduException e) {
				logger().error("Kudu table [" + table + "] open fail", e);
				return null;
			}
			return t;
		});

	}

	public boolean upsert(String table, Map<String, Object> record) {
		KuduTable t = table(table);
		if (null == t)
			return false;
		Schema schema = t.getSchema();
		List<String> keys = new ArrayList<>();
		schema.getPrimaryKeyColumns().forEach(p -> keys.add(p.getName()));
		if (record == null)
			return false;
		if (!record.keySet().containsAll(keys))
			return false;
		Upsert upsert = t.newUpsert();
		schema.getColumns().forEach(cs -> upsert(cs, record, upsert));
		try {
			OperationResponse or = session.apply(upsert);
			if (or == null) {
				return true;
			}
			boolean error = or.hasRowError();
			if (error)
				logger().error("Kudu row error: " + or.getRowError().toString());
			return error;
		} catch (KuduException ex) {
			return false;
		}
		return null;
	}

	private void upsert(ColumnSchema columnSchema, Map<String, Object> record, Upsert upsert) {
		Object field = record.get(columnSchema.getName());
		if (null != field)
			KuduCommon.generateColumnData(columnSchema.getType(), upsert.getRow(), columnSchema.getName(), field);
	}
}
