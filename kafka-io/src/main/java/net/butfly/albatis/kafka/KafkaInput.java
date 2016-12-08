package net.butfly.albatis.kafka;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import net.butfly.albacore.exception.ConfigException;

@Deprecated
public class KafkaInput extends KafkaInputBase<ReentrantLock> {
	private static final long serialVersionUID = 7617065839861658802L;

	public KafkaInput(String name, final String config, String... topic) throws ConfigException, IOException {
		super(name, config, topic);
	}

	@Override
	protected KafkaMessage fetch(KafkaStream<byte[], byte[]> stream, ReentrantLock lock,
			net.butfly.albacore.lambda.Consumer<KafkaMessage> result) {
		try {
			if (!lock.tryLock(10, TimeUnit.MILLISECONDS)) return null;
		} catch (InterruptedException e) {
			return null;
		}
		MessageAndMetadata<byte[], byte[]> e = null;
		try {
			e = stream.iterator().next();
		} catch (Exception ex) {} finally {
			lock.unlock();
		}
		if (null != e) {
			KafkaMessage km = new KafkaMessage(e);
			result.accept(km);
			return km;
		} else return null;
	}

	@Override
	protected Map<String, Map<KafkaStream<byte[], byte[]>, ReentrantLock>> parseStreams(Map<String, List<KafkaStream<byte[], byte[]>>> s) {
		Map<String, Map<KafkaStream<byte[], byte[]>, ReentrantLock>> ss = new HashMap<>();
		for (String t : s.keySet())
			for (KafkaStream<byte[], byte[]> stream : s.get(t))
				ss.compute(t, (k, v) -> {
					Map<KafkaStream<byte[], byte[]>, ReentrantLock> v1 = v == null ? new HashMap<>() : v;
					v1.put(stream, new ReentrantLock());
					return v1;
				});
		return ss;
	}
}
