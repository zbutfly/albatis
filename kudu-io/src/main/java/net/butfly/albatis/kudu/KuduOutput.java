package net.butfly.albatis.kudu;

import static net.butfly.albacore.utils.collection.Streams.map;
import static net.butfly.albacore.utils.collection.Streams.of;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import net.butfly.albacore.base.Namedly;
import net.butfly.albatis.io.KeyOutput;
import net.butfly.albatis.io.Message;

public class KuduOutput extends Namedly implements KeyOutput<String, Message> {
	public static final int SUGGEST_BATCH_SIZE = 200;
	private final KuduConnection connect;

	public KuduOutput(String name, KuduConnection conn) throws IOException {
		super(name);
		connect = conn;
		open();
	}

	@Override
	public void close() {
		KeyOutput.super.close();
		commit();
		connect.close();
	}

	@Override
	public void commit() {
		connect.commit();
	}

	@Override
	public void enqueue(String table, Stream<Message> msgs) {
		failed(of(map(msgs, m -> {
			if (m.isEmpty()) return null;
			Throwable e = connect.apply(m);
			if (null == e) {
				succeeded(1);
				return null;
			} else return m;
		}, Collectors.toList())));
	}

	@Override
	public String partition(Message v) {
		return v.table();
	}
}
