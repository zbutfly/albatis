package net.butfly.albatis.kudu;

import static net.butfly.albacore.paral.Sdream.of;
import static net.butfly.albatis.io.Rmap.Op.DELETE;
import static net.butfly.albatis.io.Rmap.Op.INSERT;
import static net.butfly.albatis.io.Rmap.Op.UPDATE;
import static net.butfly.albatis.io.Rmap.Op.UPSERT;

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

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.OddOutputBase;
import net.butfly.albatis.io.Rmap;

public class KuduOutput extends OddOutputBase<Rmap> {
	private static final long serialVersionUID = -4548392751373001781L;
	public static final int SUGGEST_BATCH_SIZE = 200;
	private final KuduConnectionBase<?, ?, ?> conn;

	public KuduOutput(String name, KuduConnectionBase<?, ?, ?> conn) throws IOException {
		super(name);
		this.conn = conn;
	}

	@Override
	public URISpec target() {
		return conn.uri();
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

	protected void regDups(Rmap m) {
		long ccc = keys.compute(m.key(), (k, cc) -> null == cc ? 1L : cc + 1);
		maxDup.accumulateAndGet(new Pair<>(m.key(), ccc), (origin, get) -> {
			if (get.v2() <= 1) return origin;
			if (null == origin || get.v2().longValue() > origin.v2().longValue()) return get;
			return origin;
		});
	}

	@Override
	public void commit() {
		conn.commit();
	}

	@Override
	protected boolean enqsafe(Rmap m) {
		if (Colls.empty(m)) return false;
		conn.apply(op(m), (op, e) -> failed(Sdream.of1(m)));
		return true;
	}

	public String status() {
		return conn.status() + "\n\tDistinct keys: " + keys.size() + ", max duplication times of key: " + maxDup.get();
	}

	private Operation op(Rmap m) {
		KuduTable t = conn.table(m.table().qualifier);
		ColumnSchema[] cols = conn.schemas(m.table().qualifier);
		if (null == t) return null;
		switch (m.op()) {
		case DELETE:
			if (null != m.key()) for (ColumnSchema cs : cols)
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
			Upsert ups = conn.table(m.table().qualifier).newUpsert();
			Object v;
			for (int i = 0; i < cols.length; i++)
				if (null != (v = m.get(cols[i].getName()))) KuduCommon.generateColumnData(cols[i].getType(), i, ups.getRow(), v);

			return ups;
		default:
			return null;
		}
	}
}
