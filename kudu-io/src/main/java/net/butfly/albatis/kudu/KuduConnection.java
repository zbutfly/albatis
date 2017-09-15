package net.butfly.albatis.kudu;

import static net.butfly.albacore.utils.collection.Streams.of;
import static net.butfly.albacore.utils.parallel.Parals.eachs;

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
	public void commit() {
		try {
			eachs(of(session.flush()), r -> {
				if (r.hasRowError()) error(r);
			});
		} catch (KuduException e) {
			logger.error("Kudu commit fail", e);
		}
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

	public Throwable apply(Message m) {
		Operation op = op(m);
		if (null != op) {
			OperationResponse or;
			try {
				or = session.apply(op);
			} catch (KuduException e) {
				if (isNonRecoverable(e)) {
					logger.error("Kudu apply fail non-recoverable", e);
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
		if (null == t) return null;
		switch (m.op()) {
		case DELETE:
			for (ColumnSchema cs : t.getSchema().getColumns())
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
			t.getSchema().getColumns().forEach(cs -> {
				Object field = m.get(cs.getName());
				if (null != field) KuduCommon.generateColumnData(cs.getType(), ups.getRow(), cs.getName(), field);
			});
			return ups;
		}
		return null;
	}
}
