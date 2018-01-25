package net.butfly.albatis.kudu;

import static net.butfly.albacore.paral.Sdream.of;
import static net.butfly.albatis.io.Message.Op.DELETE;
import static net.butfly.albatis.io.Message.Op.INSERT;
import static net.butfly.albatis.io.Message.Op.UPDATE;
import static net.butfly.albatis.io.Message.Op.UPSERT;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.Delete;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.Upsert;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.Message;
import net.butfly.albatis.io.OddOutputBase;

public class KuduOutput extends OddOutputBase<Message> {
	public static final int SUGGEST_BATCH_SIZE = 200;
	private final KuduConnectionBase<?, ?, ?> connect;

	public KuduOutput(String name, KuduConnectionBase<?, ?, ?> conn) throws IOException {
		super(name);
		connect = conn;
	}

	@Override
	public void close() {
		commit();
		super.close();
		// sdumpDups();
	}

	protected final Map<Object, Long> keys = Maps.of();
	protected final AtomicReference<Pair<Object, Long>> maxDup = new AtomicReference<>();

	protected void dumpDups() {
		if (keys.isEmpty()) return;
		Map<Object, Long> dups = of(keys).filter(e -> e.getValue() > 1).partitions(e -> e.getKey(), e -> e.getValue());
		try (FileOutputStream fs = new FileOutputStream("duplicated-keys.dump");
				OutputStreamWriter ow = new OutputStreamWriter(fs);
				BufferedWriter ww = new BufferedWriter(ow);) {
			for (Entry<Object, Long> e : dups.entrySet())
				ww.write(e.getKey() + ": " + e.getValue() + "\n");
		} catch (IOException e) {
			logger().error("Duplicated keys dump file open failed", e);
		}
	}

	protected void regDups(Message m) {
		long ccc = keys.compute(m.key(), (k, cc) -> null == cc ? 1L : cc + 1);
		maxDup.accumulateAndGet(new Pair<>(m.key(), ccc), (origin, get) -> {
			if (get.v2() <= 1) return origin;
			if (null == origin || get.v2().longValue() > origin.v2().longValue()) return get;
			return origin;
		});
	}

	@Override
	public void commit() {
		connect.commit();
	}

	@Override
	protected boolean enqueue0(Message m) {
		if (null == m || m.isEmpty()) return false;
		connect.apply(op(m), (op, e) -> failed(Sdream.of1(m)));
		return true;
	}

	public String status() {
		return connect.status() + "\n\tDistinct keys: " + keys.size() + ", max duplication times of key: " + maxDup.get();
	}

	private Operation op(Message m) {
		KuduTable t = connect.table(m.table());
		Map<String, ColumnSchema> cols = connect.schemas(m.table());
		ColumnSchema c;
		if (null == t) return null;
		switch (m.op()) {
		case DELETE:
			if (null != m.key()) for (ColumnSchema cs : cols.values())
				if (cs.isKey()) {
					Delete del = t.newDelete();
					del.getRow().addString(cs.getName(), m.key().toString());
					return del;
				}
			return null;
		case INSERT:
		case UPDATE:
		case UPSERT:
			// regDups(m);
			Upsert ups = connect.table(m.table()).newUpsert();
			for (String f : m.keySet())
				if (null != (c = cols.get(f.toLowerCase())))//
					KuduCommon.generateColumnData(c.getType(), ups.getRow(), c.getName(), m.get(f));
			return ups;
		default:
			return null;
		}
	}
}
