package net.butfly.albatis.kudu;

import static net.butfly.albacore.utils.collection.Streams.of;
import static net.butfly.albacore.utils.parallel.Parals.eachs;

import java.io.IOException;
import java.util.ArrayList;
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
import com.hzcominfo.albatis.Albatis;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Configs;

@SuppressWarnings("unchecked")
public class KuduSyncConnection extends KuduConnection<KuduSyncConnection, KuduClient, KuduSession> {
	public KuduSyncConnection(URISpec kuduUri) throws IOException {
		super(kuduUri, r -> new KuduClient.KuduClientBuilder(kuduUri.getHost()).build());
		session = client().newSession();
		session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND);
		session.setTimeoutMillis(Long.parseLong(Configs.get(Albatis.Props.PROP_KUDU_TIMEOUT, "2000")));
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
		opCount.incrementAndGet();
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
			(r ? succCount : failCount).incrementAndGet();
		}
	}

	@Override
	public void table(String name, List<ColumnSchema> cols, boolean autoKey) {
		int buckets = Integer.parseInt(System.getProperty(Albatis.Props.PROP_KUDU_TABLE_BUCKETS, "24"));
		logger.info("Kudu table constructing, with bucket [" + buckets + "], can be defined by [-D" + Albatis.Props.PROP_KUDU_TABLE_BUCKETS
				+ "=400]");
		try {
			if (client().tableExists(name)) {
				logger.info("Kudu table [" + name + "] existed, will be droped.");
				client().deleteTable(name);
			}
			List<String> keys = new ArrayList<>();
			for (ColumnSchema c : cols)
				if (c.isKey()) keys.add(c.getName());
				else break;
			logger.info("Kudu table [" + name + "] will be created with keys: [" + Joiner.on(',').join(keys) + "].");
			CreateTableOptions opts = new CreateTableOptions().addHashPartitions(keys, buckets);
			client().createTable(name, new Schema(cols), opts);
			logger.info("Kudu table [" + name + "] created successfully.");
		} catch (KuduException e) {
			logger.error("Build kudu table fail.", e);
			throw new RuntimeException(e);
		}
	}
}
