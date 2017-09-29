package net.butfly.albatis.kudu;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

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

@SuppressWarnings("unchecked")
public class KuduConnection extends NoSqlConnection<KuduClient> {
	private static final Logger logger = Logger.getLogger(KuduConnection.class);
	private final KuduSession session;
	private Thread failHandler;

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
		return tables.computeIfAbsent(table, t -> {
			try {
				return client().openTable(t);
			} catch (KuduException e) {
				logger().error("Kudu table [" + table + "] open fail", e);
				return null;
			}
			return t;
		});

	}

	private static final Map<String, Map<String, ColumnSchema>> SCHEMAS_CI = new ConcurrentHashMap<>();

	private Map<String, ColumnSchema> schemas(String table) {
		return SCHEMAS_CI.computeIfAbsent(table, t -> {
			Map<String, ColumnSchema> m = table(t).getSchema().getColumns().parallelStream()//
					.collect(Collectors.toConcurrentMap(c -> c.getName().toLowerCase(), c -> c));
			return m;
		});
	}

	public Throwable apply(Message m) {
		Operation op = op(m);
		if (null != op) {
			OperationResponse or;
			try {
				or = session.apply(op);
			} catch (KuduException e) {
				if (isNonRecoverable(e)) {
					logger.error("Kudu apply fail non-recoverable: " + e.getMessage());
					return null;
				} else return e;
			}
			if (or != null && or.hasRowError()) return new IOException(or.getRowError().toString());
		}
		return null;
	}

	private static final Class<? extends KuduException> c;
	static {
		Class<? extends KuduException> cc = null;
		try {
			cc = (Class<? extends KuduException>) Class.forName("org.apache.kudu.client.NonRecoverableException");
		} catch (ClassNotFoundException e) {} finally {
			c = cc;
		}
	}

	public static boolean isNonRecoverable(KuduException e) {
		return null != c && c.isAssignableFrom(e.getClass());
	}

	private Operation op(Message m) {
		KuduTable t = table(m.table());
		Map<String, ColumnSchema> cols = schemas(m.table());
		ColumnSchema c;
		if (null == t) return null;
		switch (m.op()) {
		case DELETE:
			for (ColumnSchema cs : cols.values())
				if (cs.isKey()) {
					Delete del = t.newDelete();
					del.getRow().addString(cs.getName(), m.key());
					return del;
				}
			return null;
		case INSERT:
		case UPDATE:
		case UPSERT:
			Upsert ups = table(m.table()).newUpsert();
			for (String f : m.keySet())
				if (null != (c = cols.get(f.toLowerCase())))//
					KuduCommon.generateColumnData(c.getType(), ups.getRow(), c.getName(), m.get(f));
			return ups;
		default:
			return null;
		}
	}
}
