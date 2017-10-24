package net.butfly.albatis.kudu;

import static net.butfly.albacore.utils.collection.Streams.of;
import static net.butfly.albacore.utils.parallel.Parals.eachs;
import static net.butfly.albacore.utils.parallel.Parals.listen;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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

public class KuduAsyncConnection extends KuduConnection<KuduAsyncConnection, AsyncKuduClient, AsyncKuduSession> {
	public KuduAsyncConnection(URISpec kuduUri) throws IOException {
		super(kuduUri, r -> new AsyncKuduClient.AsyncKuduClientBuilder(kuduUri.getHost()).build());
		session = client().newSession();
		session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND);
		session.setTimeoutMillis(10000);
	}

	@Override
	public void commit() {
		try {
			eachs(of(session.flush().join()), r -> error(r));
		} catch (Exception e) {
			logger.error("Kudu commit fail", e);
		}
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
	public void apply(Operation op, BiConsumer<Operation, Throwable> error) {
		if (null == op) return;
		Deferred<OperationResponse> or;
		try {
			or = session.apply(op);
		} catch (KuduException e) {
			if (isNonRecoverable(e)) logger.error("Kudu apply fail non-recoverable: " + e.getMessage());
			else error.accept(op, e);
			return;
		}
		listen(() -> {
			OperationResponse r;
			try {
				r = or.join();
			} catch (Exception e) {
				error.accept(op, e);
				return;
			}
			if (r != null && r.hasRowError()) error.accept(op, new IOException(r.getRowError().getErrorStatus().toString()));
		});

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
}
