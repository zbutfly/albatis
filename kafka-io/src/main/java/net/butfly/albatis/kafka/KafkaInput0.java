package net.butfly.albatis.kafka;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.lambda.Consumer;

@Deprecated
public class KafkaInput0 extends KafkaInputBase<ReentrantLock> {

	public KafkaInput0(String name, final String config, String... topic) throws ConfigException, IOException {
		super(name, config, topic);
		for (String t : raws.keySet())
			for (KafkaStream<byte[], byte[]> stream : raws.get(t))
				streams.compute(t, (k, v) -> {
					Map<KafkaStream<byte[], byte[]>, ReentrantLock> v1 = v == null ? new HashMap<>() : v;
					v1.put(stream, new ReentrantLock());
					return v1;
				});
	}

	@Override
	protected KafkaMessage fetch(KafkaStream<byte[], byte[]> stream, ReentrantLock lock, Consumer<KafkaMessage> result) {
		try {
			if (!lock.tryLock(10, TimeUnit.MILLISECONDS)) return null;
		} catch (InterruptedException e) {
			logger.warn("[" + name() + "] fetching interrupted.");
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
}
