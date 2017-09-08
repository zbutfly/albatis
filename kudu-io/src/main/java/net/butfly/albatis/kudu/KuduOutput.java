package net.butfly.albatis.kudu;

import java.io.IOException;
import java.util.stream.Stream;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.io.EnqueueException;
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
	public long enqueue(String table, Stream<Message> msgs) throws EnqueueException {
		EnqueueException ex = new EnqueueException();
		msgs.parallel().filter(Streams.NOT_EMPTY_MAP).forEach(m -> {
			try {
				connect.apply(m);
				ex.success(1);
			} catch (IOException e) {
				ex.fail(m, e);
			}
		});
		if (!ex.empty()) throw ex;
		else return ex.success();
	}

	@Override
	public String partition(Message v) {
		return v.key();
	}
}
