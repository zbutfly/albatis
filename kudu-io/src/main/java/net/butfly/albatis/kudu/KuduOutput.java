package net.butfly.albatis.kudu;

import static net.butfly.albacore.utils.collection.Streams.map;
import static net.butfly.albacore.utils.collection.Streams.of;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import net.butfly.albacore.base.Namedly;
import net.butfly.albatis.io.KeyOutput;
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
