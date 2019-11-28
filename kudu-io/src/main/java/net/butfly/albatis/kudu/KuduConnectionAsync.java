package net.butfly.albatis.kudu;

import static net.butfly.albacore.paral.Sdream.of;
import static net.butfly.albatis.kudu.KuduConnection.KERBEROS_CONF_PATH;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import com.stumbleupon.async.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.AsyncKuduSession;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.SessionConfiguration.FlushMode;

import com.stumbleupon.async.Deferred;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.io.lambda.BiConsumer;
import net.butfly.albacore.paral.Task;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.Pair;

public class KuduConnectionAsync extends KuduConnectionBase<KuduConnectionAsync, AsyncKuduClient, AsyncKuduSession> {
	private BlockingQueue<Pair<AsyncKuduSession, Pair<Operation, Deferred<OperationResponse>>>> PENDINGS = new LinkedBlockingQueue<>(1000);
	private final Thread pendingHandler;
	private final int STUMBLEUPON_TIMEOUT = Integer.parseInt(Configs.gets(KuduProps.STUMBLEUPON_TIMEOUT, "500"));

	public KuduConnectionAsync(URISpec kuduUri) throws IOException {
		super(kuduUri);

		pendingHandler = new Thread(() -> {
			List<Pair<AsyncKuduSession, Pair<Operation, Deferred<OperationResponse>>>> m = new CopyOnWriteArrayList<Pair<AsyncKuduSession, Pair<Operation, Deferred<OperationResponse>>>>();
			do {
				while (PENDINGS.drainTo(m, 1000) > 0) for (Pair<AsyncKuduSession, Pair<Operation, Deferred<OperationResponse>>> d : m) process(d.v1(), d.v2().v1(), d.v2().v2(),
						this::error);
			} while (Task.waitSleep());
		}, "KuduErrorHandler[" + kuduUri.toString() + "]");
		session = client.newSession();
		session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND);
		session.setTimeoutMillis(Long.parseLong(Configs.get(KuduProps.TIMEOUT, "2000")));
		session.setFlushInterval(1000);
		session.setMutationBufferSpace(5);
		pendingHandler.setDaemon(true);
		pendingHandler.start();
	}

	@Override
	protected AsyncKuduClient initialize(URISpec uri) {
		if(null != uri.getParameter("kerberos")){ KERBEROS_CONF_PATH = uri.getParameter("kerberos"); }
		if(null!=KERBEROS_CONF_PATH){
			new Kerberos(KERBEROS_CONF_PATH);
			try {
				return UserGroupInformation.getLoginUser().doAs((PrivilegedAction<AsyncKuduClient>) () -> {
					return new  AsyncKuduClient.AsyncKuduClientBuilder(uri.getHost()).build();
				});
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return new AsyncKuduClient.AsyncKuduClientBuilder(uri.getHost()).build();
	}

	@Override
	protected KuduTable openTable(String table) {
		try {
			return client.openTable(table).join();
		} catch (Exception e) {
			logger().error("Kudu table open fail", e);
			return null;
		}
	}

	@Override
	public boolean apply(Operation op, BiConsumer<Operation, Throwable> error) {
		if (null == op) return false;
		Deferred<OperationResponse> or;
		try {
			or = session.apply(op);
		} catch (KuduException e) {
			if (isNonRecoverable(e)) logger.error("Kudu apply fail non-recoverable: " + e.getMessage());
			else error.accept(op, e);
			return false;
		}
		return process(session, op, or, error);
	}

	private final AtomicLong millis = new AtomicLong(), ops = new AtomicLong();

	private boolean process(AsyncKuduSession session, Operation op, Deferred<OperationResponse> or, BiConsumer<Operation, Throwable> error) {
		OperationResponse r;
		try {
			r = or.joinUninterruptibly(STUMBLEUPON_TIMEOUT);
		} catch (TimeoutException e) {
			logger.trace("Kudu op response timeout after " + STUMBLEUPON_TIMEOUT + "ms");
			if (!PENDINGS.offer(new Pair<>(session, new Pair<>(op, or)))) //
				logger.warn("Kudu op response timeout and pending queue full, dropped: \n\t" + op.toString(), e);
			return true;
		} catch (Exception e) {
			//error.accept(op, e);
			logger.error("Unkown error, droped: \n\t" + op.toString(), e);
			return false;
		}
		if (r != null && r.hasRowError()) {
			error.accept(op, new IOException(r.getRowError().getErrorStatus().toString()));
			return false;
		} else {
//			logger.info("right success!" + op.toString());
			if (session != null && !session.isClosed()) session.close();
			if (logger.isTraceEnabled()) {
				long ms = millis.addAndGet(r.getElapsedMillis());
				long c = ops.incrementAndGet();
				if (c % 1000 == 0) //
					logger.trace("Avg row kudu op spent: " + c + " objs, " + ms + " ms, " + (c * 1000.0 / ms) + " avg objs/ms.");
			}
			return true;
		}
	}

	@Override
	public void destruct(String table) {
		try {
			if (client.tableExists(table).join()) {
				logger.warn("Kudu table [" + table + "] exised and dropped.");
				client.deleteTable(table);
			}
		} catch (Exception ex) {}
	}

	@Override
	protected void construct(String table, ColumnSchema... cols) {
		try {
			if (client.tableExists(table).join()) {
				logger.warn("Ask for creating new table but existed and not droped, ignore");
				return;
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		List<String> keys = new ArrayList<>();
		for (ColumnSchema c : cols) if (c.isKey()) keys.add(c.getName());
		else break;

		int buckets = Integer.parseInt(System.getProperty(KuduProps.TABLE_BUCKETS, "24"));
		String v = Configs.get(KuduProps.TABLE_REPLICAS);
		int replicas = null == v ? -1 : Integer.parseInt(v);
		String info = "with bucket [" + buckets + "], can be defined by [-D" + KuduProps.TABLE_BUCKETS + "=8(default value)]";
		if (replicas > 0) info = info + ", with replicas [" + replicas + "], can be defined by [-D" + KuduProps.TABLE_REPLICAS
				+ "=xx(no default value)]";
		logger.info("Kudu table [" + table + "] will be created with keys: [" + String.join(",", keys) + "], " + info);
		CreateTableOptions opts = new CreateTableOptions().addHashPartitions(keys, buckets);
		if (replicas > 0) opts = opts.setNumReplicas(replicas);
		try {
			client.createTable(table, new Schema(Arrays.asList(cols)), opts).join();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		logger.info("Kudu table [" + table + "] created successfully.");
	}

	@Override
	public void commit() {
		List<OperationResponse> v;
		try {
			v = session.flush().join();
		} catch (Exception e) {
			logger.error("Kudu commit fail", e);
			return;
		}
		of(v).eachs(r -> {
			if (r.hasRowError()) error(r);
		});
	}
}
