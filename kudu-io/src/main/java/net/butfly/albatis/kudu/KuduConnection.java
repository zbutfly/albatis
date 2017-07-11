package net.butfly.albatis.kudu;

import static net.butfly.albacore.paral.Sdream.of;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.CreateTableOptions;
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

import com.google.common.base.Joiner;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Configs;

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

	public KuduConnection(String kuduUri, Map<String, String> props) throws IOException {
		this(new URISpec(kuduUri), props);
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
	protected KuduTable openTable(String table) {
		try {
			return client().openTable(table);
		} catch (KuduException e) {
			logger().error("Kudu table open fail", e);
			return null;
		}
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

	@Override
	public boolean apply(Operation op, BiConsumer<Operation, Throwable> error) {
		if (null == op) return false;
		// opCount.incrementAndGet();
		boolean r = true;
		OperationResponse or = null;
		try {
			try {
				or = session.apply(op);
			} catch (KuduException e) {
				if (isNonRecoverable(e)) logger.error("Kudu apply fail non-recoverable: " + e.getMessage());
				else error.accept(op, e);
				return (r = false);
			}
			if (null == or) return (r = true);
			if (!(r = !or.hasRowError())) error.accept(op, new IOException(or.getRowError().toString()));
			return r;
		} finally {
			// (r ? succCount : failCount).incrementAndGet();
		}
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
			logger.warn("Kudu table [" + name + "] drop fail", ex);
		}
	}

	private void upsert(ColumnSchema columnSchema, Map<String, Object> record, Upsert upsert) {
		Object field = record.get(columnSchema.getName());
		if (null != field)
			KuduCommon.generateColumnData(columnSchema.getType(), upsert.getRow(), columnSchema.getName(), field);
	}
}
