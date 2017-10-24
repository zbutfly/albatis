package net.butfly.albatis.kudu;

import static net.butfly.albacore.utils.collection.Streams.of;
import static net.butfly.albacore.utils.collection.Streams.toMap;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.Delete;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.Upsert;

import net.butfly.albacore.base.Namedly;
import net.butfly.albatis.io.Message;
import net.butfly.albatis.io.Output;

public class KuduOutput extends Namedly implements Output<Message> {
	public static final int SUGGEST_BATCH_SIZE = 200;
	private final KuduConnection<?, ?, ?> connect;

	public KuduOutput(String name, KuduConnection<?, ?, ?> conn) throws IOException {
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
	public void enqueue(Stream<Message> msgs) {
		Map<Operation, Message> ops = toMap(msgs, m -> op(m), m -> m);
		connect.apply(of(ops.keySet()), (op, e) -> failed(of(ops.get(op))));
	}

	private Operation op(Message m) {
		KuduTable t = connect.table(m.table());
		Map<String, ColumnSchema> cols = connect.schemas(m.table());
		ColumnSchema c;
		if (null == t) return null;
		switch (m.op()) {
		case DELETE:
			for (ColumnSchema cs : cols.values())
				if (cs.isKey()) {
					Delete del = t.newDelete();
					del.getRow().addString(cs.getName(), m.key());
					return del;
				}
			return null;
		case INSERT:
		case UPDATE:
		case UPSERT:
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
