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

import com.google.common.base.Joiner;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Configs;

@SuppressWarnings("unchecked")
public class KuduConnection extends KuduConnectionBase<KuduConnection, KuduClient, KuduSession> {
	public KuduConnection(URISpec kuduUri) throws IOException {
		super(kuduUri, r -> new KuduClient.KuduClientBuilder(kuduUri.getHost()).build());
		session = client().newSession();
		session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND);
		session.setTimeoutMillis(Long.parseLong(Configs.get(KuduProps.TIMEOUT, "2000")));
	}

	@Override
	public void commit() {
		List<OperationResponse> v;
		try {
			v = session.flush();
		} catch (KuduException e) {
			logger.error("Kudu commit fail", e);
			return;
		}
		of(v).eachs(r -> {
			if (r.hasRowError()) error(r);
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

	@Override
	public void tableDrop(String name) {
		try {
			if (!client().tableExists(name)) logger.warn("Kudu table [" + name + "] not exised, need not dropped.");
			else {
				logger.warn("Kudu table [" + name + "] exised and dropped.");
				client().deleteTable(name);
			}
		} catch (KuduException ex) {
			logger.warn("Kudu table [" + name + "] drop fail", ex);
		}
	}

	@Override
	public void tableCreate(String name, boolean drop, ColumnSchema... cols) {
		if (drop) tableDrop(name);
		try {
			if (client().tableExists(name)) {
				logger.warn("Ask for creating new table but existed and not droped, ignore");
				return;
			}
		} catch (KuduException e) {
			throw new RuntimeException(e);
		}
		List<String> keys = new ArrayList<>();
		for (ColumnSchema c : cols)
			if (c.isKey()) keys.add(c.getName());
			else break;

		int buckets = Integer.parseInt(System.getProperty(KuduProps.TABLE_BUCKETS, "24"));
		String v = Configs.get(KuduProps.TABLE_REPLICAS);
		int replicas = null == v ? -1 : Integer.parseInt(v);
		String info = "with bucket [" + buckets + "], can be defined by [-D" + KuduProps.TABLE_BUCKETS + "=8(default value)]";
		if (replicas > 0) info = info + ", with replicas [" + replicas + "], can be defined by [-D" + KuduProps.TABLE_REPLICAS
				+ "=xx(no default value)]";
		logger.info("Kudu table [" + name + "] will be created with keys: [" + Joiner.on(',').join(keys) + "], " + info);
		CreateTableOptions opts = new CreateTableOptions().addHashPartitions(keys, buckets);
		if (replicas > 0) opts = opts.setNumReplicas(replicas);
		try {
			client().createTable(name, new Schema(Arrays.asList(cols)), opts);
		} catch (KuduException e) {
			throw new RuntimeException(e);
		}
		logger.info("Kudu table [" + name + "] created successfully.");
	}
}
