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
		open();
	}

	@Override
	public void close() {
		commit();
		connect.close();
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
