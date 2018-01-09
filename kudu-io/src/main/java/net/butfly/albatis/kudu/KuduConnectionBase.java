package net.butfly.albatis.kudu;

import static net.butfly.albacore.paral.Sdream.of;
import static net.butfly.albacore.paral.Task.waitSleep;

import java.io.IOException;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.RowError;
import org.apache.kudu.client.RowErrorsAndOverflowStatus;
import org.apache.kudu.client.SessionConfiguration;

import com.hzcominfo.albatis.nosql.NoSqlConnection;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;

@SuppressWarnings("unchecked")
public abstract class KuduConnectionBase<C extends KuduConnectionBase<C, KC, S>, KC extends AutoCloseable, S extends SessionConfiguration>
		extends NoSqlConnection<KC> {
	protected static final Logger logger = Logger.getLogger(KuduConnectionBase.class);
	private static final Map<String, KuduTable> tables = Maps.of();
	private static final Map<String, Map<String, ColumnSchema>> SCHEMAS_CI = Maps.of();
	private final Thread failHandler;
	protected S session;

	protected KuduConnectionBase(URISpec kuduUri, Function<URISpec, KC> clienting) throws IOException {
		super(kuduUri, clienting, "kudu");

		failHandler = new Thread(() -> {
			do {
				errors();
			} while (waitSleep());
		}, "KuduErrorHandler[" + kuduUri.toString() + "]");
		failHandler.setDaemon(true);
		failHandler.start();
	}

	// protected final AtomicLong opCount = new AtomicLong(), succCount = new
	// AtomicLong(), failCount = new AtomicLong();

	public abstract boolean apply(Operation op, BiConsumer<Operation, Throwable> error);

	public String status() {
		return "[Kudu Status]: ";
		// + opCount.get() + " input, " + succCount + " success, " + failCount +
		// " failed.";
	}

	protected final void errors() {
		if (null == session) return;
		RowErrorsAndOverflowStatus errs = session.getPendingErrors();
		if (null == errs) return;
		RowError[] rows = session.getPendingErrors().getRowErrors();
		if (null == rows || rows.length == 0) return;
		of(rows).map(RowError::getOperation).eachs(op -> this.apply(op, this::error));
	}

	protected final void error(Operation op, Throwable cause) {
		Exeter.of().submit((Runnable) () -> apply(op, this::error));
	}

	protected final void error(OperationResponse r) {
		error(r.getRowError().getOperation(), new IOException(r.getRowError().getErrorStatus().toString()));
	}

	@Override
	public void close() {
		try {
			super.close();
		} catch (IOException e) {
			logger.error("Close failure", e);
		}
		try {
			client().close();
		} catch (Exception e) {
			logger.error("Close failure", e);
		}
	}

	public final KuduTable table(String table) {
		return tables.computeIfAbsent(table, this::openTable);
	}

	protected abstract KuduTable openTable(String table);

	Map<String, ColumnSchema> schemas(String table) {
		return SCHEMAS_CI.computeIfAbsent(table, t -> {
			Map<String, ColumnSchema> m = table(t).getSchema().getColumns().parallelStream()//
					.collect(Collectors.toConcurrentMap(c -> c.getName().toLowerCase(), c -> c));
			return m;
		});
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

	public abstract void commit();

	public abstract void tableDrop(String table);

	public abstract void tableCreate(String name, boolean drop, ColumnSchema... cols);
}
