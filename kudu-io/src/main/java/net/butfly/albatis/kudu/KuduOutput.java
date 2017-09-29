package net.butfly.albatis.kudu;

import java.io.IOException;
import java.util.stream.Stream;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.utils.collection.Streams;
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
		msgs.parallel().filter(Streams.NOT_EMPTY_MAP).forEach(m -> {
			Throwable e = connect.apply(m);
			if (null == e) succeeded(1);
			else failed(Streams.of(new Message[] { m }));
		});
	}

	@Override
	public String partition(Message v) {
		return v.key();
	}
}
