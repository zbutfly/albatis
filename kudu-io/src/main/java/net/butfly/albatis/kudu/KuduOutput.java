package net.butfly.albatis.kudu;

import java.io.IOException;
import java.util.stream.Stream;

import org.apache.kudu.client.KuduClient;

import net.butfly.albacore.io.EnqueueException;
import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.io.KeyOutput;
import net.butfly.albatis.io.Message;

public class KuduOutput extends KeyOutput<String, Message> {
	public static final int SUGGEST_BATCH_SIZE = 200;
	private final KuduConnection connect;

	public KuduOutput(String name, URISpec uri) throws IOException {
		super(name);
		connect = new KuduConnection(uri, null);
		open();
	}

	@Override
	public void close() {
		super.close();
		commit();
		connect.close();
	}

	@Override
	public void commit() {
		connect.commit();
	}

	@Override
	public long enqueue(String table, Stream<Message> values) throws EnqueueException {
		EnqueueException ex = new EnqueueException();
		values.parallel().filter(r -> r != null && !r.isEmpty()).forEach(r -> {
			if (connect.upsert(table, r)) ex.success(1);
			else ex.fail(r, new RuntimeException());
		});
		if (ex.empty()) return ex.success();
		else throw ex;
	}

	@Deprecated
	public KuduClient client() {
		return connect.client();
	}

	@Override
	protected String partitionize(Message v) {
		return v.key();
	}
}
