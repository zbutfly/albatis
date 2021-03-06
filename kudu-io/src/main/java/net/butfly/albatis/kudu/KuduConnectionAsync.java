package net.butfly.albatis.kudu;

import static net.butfly.albacore.utils.collection.Streams.of;
import static net.butfly.albacore.utils.parallel.Parals.eachs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

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

import com.google.common.base.Joiner;
import com.hzcominfo.albatis.Albatis;
import com.stumbleupon.async.Deferred;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.parallel.Concurrents;

@Deprecated
public class KuduConnectionAsync extends KuduConnBase<KuduConnectionAsync, AsyncKuduClient, AsyncKuduSession> {
	private BlockingQueue<Pair<Operation, Deferred<OperationResponse>>> PENDINGS = new LinkedBlockingQueue<>(1000);
	private final Thread pendingHandler;

	public KuduConnectionAsync(URISpec kuduUri) throws IOException {
		super(kuduUri, r -> new AsyncKuduClient.AsyncKuduClientBuilder(kuduUri.getHost()).build());
		session = client().newSession();
		session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND);
		session.setTimeoutMillis(Long.parseLong(Configs.get(Albatis.Props.PROP_KUDU_TIMEOUT, "2000")));
		session.setFlushInterval(1000);
		session.setMutationBufferSpace(5);

		pendingHandler = new Thread(() -> {
			List<Pair<Operation, Deferred<OperationResponse>>> m = new CopyOnWriteArrayList<Pair<Operation, Deferred<OperationResponse>>>();
			do {
				while (PENDINGS.drainTo(m, 1000) > 0)
					for (Pair<Operation, Deferred<OperationResponse>> d : m)
						process(d.v1(), d.v2(), this::error);
			} while (Concurrents.waitSleep());
		}, "KuduErrorHandler[" + kuduUri.toString() + "]");
		pendingHandler.setDaemon(true);
		pendingHandler.start();
	}

	@Override
	protected KuduTable openTable(String table) {
		try {
			return client().openTable(table).join();
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
		return process(op, or, error);
	}

	private final AtomicLong millis = new AtomicLong(), ops = new AtomicLong();

	private boolean process(Operation op, Deferred<OperationResponse> or, BiConsumer<Operation, Throwable> error) {
		OperationResponse r;
		try {
			r = or.joinUninterruptibly(500);
		} catch (TimeoutException e) {
			if (!PENDINGS.offer(new Pair<>(op, or))) //
				logger.warn("Kudu op response timeout and pending queue full, dropped: \n\t" + op.toString());
			return true;
		} catch (Exception e) {
			error.accept(op, e);
			return false;
		}
		if (r != null && r.hasRowError()) {
			error.accept(op, new IOException(r.getRowError().getErrorStatus().toString()));
			return false;
		} else {
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
	public void table(String name, List<ColumnSchema> cols, boolean autoKey) {
		int buckets = Integer.parseInt(System.getProperty(Albatis.Props.PROP_KUDU_TABLE_BUCKETS, "24"));
		logger.info("Kudu table constructing, with bucket [" + buckets + "], can be defined by [-D" + Albatis.Props.PROP_KUDU_TABLE_BUCKETS
				+ "=400]");
		try {
			if (client().tableExists(name).join()) {
				logger.info("Kudu table [" + name + "] existed, will be droped.");
				client().deleteTable(name).join();
			}
			List<String> keys = new ArrayList<>();
			for (ColumnSchema c : cols)
				if (c.isKey()) keys.add(c.getName());
				else break;
			logger.info("Kudu table [" + name + "] will be created with keys: [" + Joiner.on(',').join(keys) + "].");
			CreateTableOptions opts = new CreateTableOptions().addHashPartitions(keys, buckets);
			client().createTable(name, new Schema(cols), opts).join();
			logger.info("Kudu table [" + name + "] created successfully.");
		} catch (Exception e) {
			logger.error("Build kudu table fail.", e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public void commit() {
		try {
			eachs(of(session.flush().join()), r -> error(r));
		} catch (Exception e) {
			logger.error("Kudu commit fail", e);
		}
	}
}
