package net.butfly.albatis.kudu;

import static net.butfly.albacore.utils.collection.Streams.of;
import static net.butfly.albacore.utils.parallel.Parals.listen;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.SessionConfiguration;

import com.hzcominfo.albatis.nosql.NoSqlConnection;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albacore.utils.parallel.Concurrents;
import net.butfly.albacore.utils.parallel.Parals;

@SuppressWarnings("unchecked")
public abstract class KuduConnection<C extends KuduConnection<C, KC, S>, KC extends AutoCloseable, S extends SessionConfiguration> extends
		NoSqlConnection<KC> {
	protected static final Logger logger = Logger.getLogger(KuduConnection.class);
	private static final Map<String, KuduTable> tables = new ConcurrentHashMap<>();
	private static final Map<String, Map<String, ColumnSchema>> SCHEMAS_CI = new ConcurrentHashMap<>();
	private final Thread failHandler;
	protected S session;

	protected KuduConnection(URISpec kuduUri, Function<URISpec, KC> clienting) throws IOException {
		super(kuduUri, clienting, "kudu");

		failHandler = new Thread(() -> {
			do {
				errors();
			} while (Concurrents.waitSleep());
		}, "KuduErrorHandler[" + kuduUri.toString() + "]");
		failHandler.setDaemon(true);
		failHandler.start();
	}

	public abstract void apply(Operation op, BiConsumer<Operation, Throwable> error);

	public void apply(Stream<Operation> op, BiConsumer<Operation, Throwable> error) {
		Parals.eachs(op, o -> apply(op, error));
	}

	protected final void errors() {
		if (session.getPendingErrors() != null) apply(of(session.getPendingErrors().getRowErrors()).map(e -> e.getOperation()),
				this::error);
	}

	protected final void error(Operation op, Throwable cause) {
		listen(() -> apply(op, this::error));

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

	public abstract void table(String name, List<ColumnSchema> cols, boolean autoKey);
}
