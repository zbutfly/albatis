package net.butfly.albatis.kudu;

import static net.butfly.albacore.paral.Sdream.of;
import static net.butfly.albacore.paral.Task.waitSleep;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import net.butfly.albatis.ddl.Qualifier;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.RowError;
import org.apache.kudu.client.RowErrorsAndOverflowStatus;
import org.apache.kudu.client.SessionConfiguration;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.io.lambda.BiConsumer;
import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.DataConnection;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Rmap;

@SuppressWarnings("unchecked")
public abstract class KuduConnectionBase<C extends KuduConnectionBase<C, KC, S>, KC extends AutoCloseable, S extends SessionConfiguration>
		extends DataConnection<KC> {
	protected static final Logger logger = Logger.getLogger(KuduConnectionBase.class);
	private static final Map<String, KuduTable> tables = Maps.of();
	private static final Map<String, ColumnSchema[]> SCHEMAS_CI = Maps.of();
	private final Thread failHandler;
	protected S session;

	protected KuduConnectionBase(URISpec kuduUri) throws IOException {
		super(kuduUri, "kudu");

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
			client.close();
		} catch (Exception e) {
			logger.error("Close failure", e);
		}
	}

	public final KuduTable table(String table) {
		return tables.computeIfAbsent(table, this::openTable);
	}

	protected abstract KuduTable openTable(String table);

	ColumnSchema[] schemas(String table) {
		return SCHEMAS_CI.computeIfAbsent(table, t -> {
			return table(t).getSchema().getColumns().toArray(new ColumnSchema[0]);
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

	@Override
	public abstract void destruct(String table);

	protected abstract void construct(String qualifier, ColumnSchema... cols);

	@Override
	public final void construct(Qualifier qualifier, FieldDesc... fields) {
		construct(qualifier.toString(), ColBuild.buildColumns(false, fields));
	}

	public static class Driver implements net.butfly.albatis.Connection.Driver<KuduConnection> {
		static {
			DriverManager.register(new Driver());
		}

		@Override
		public KuduConnection connect(URISpec uriSpec) throws IOException {
			return new KuduConnection(uriSpec);
		}

		@Override
		public List<String> schemas() {
			return Colls.list("kudu");
		}
	}

	public static class AsyncDriver implements net.butfly.albatis.Connection.Driver<KuduConnectionAsync> {
		static {
			DriverManager.register(new AsyncDriver());
		}

		@Override
		public KuduConnectionAsync connect(URISpec uriSpec) throws IOException {
			return new KuduConnectionAsync(uriSpec);
		}

		@Override
		public List<String> schemas() {
			return Colls.list("kudu:async");
		}

	}

	@Override
	public Input<Rmap> inputRaw(TableDesc... tables) throws IOException {
		KuduInput input = new KuduInput("KuduInput", this);
		for (TableDesc table : tables) {
			input.table(table);
		}
		return input;
	}

	@Override
	public KuduOutput outputRaw(TableDesc... tables) throws IOException {
		return new KuduOutput("KuduOutput", this);
	}
}
