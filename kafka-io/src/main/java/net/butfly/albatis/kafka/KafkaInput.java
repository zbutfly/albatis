package net.butfly.albatis.kafka;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import net.butfly.albacore.exception.ConfigException;

public class KafkaInput extends KafkaInputBase<ReentrantLock> {
	private static final long serialVersionUID = 7617065839861658802L;

	public KafkaInput(String name, final String config, String... topic) throws ConfigException, IOException {
		super(name, config, (s, i) -> new ReentrantLock(), topic);
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
}
