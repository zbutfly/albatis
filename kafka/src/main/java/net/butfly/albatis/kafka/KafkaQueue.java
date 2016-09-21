package net.butfly.albatis.kafka;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.leansoft.bigqueue.BigQueueImpl;
import com.leansoft.bigqueue.IBigQueue;

import scala.Tuple2;

class KafkaQueue implements AutoCloseable {
	private static final Logger logger = LoggerFactory.getLogger(KafkaQueue.class);
	private static final long WAIT_MS = 1000;

	protected Map<String, IBigQueue> queues;
	protected long batchSize;
	private boolean closing;
	private String dataFolder;

	public void enqueue(String topic, byte[] message) {
		if (!closing) try {
			queue(topic).enqueue(encode(topic, message));
		} catch (IOException e) {
			logger.error("Message enqueue/serialize to local pool failure.", e);
		}
	}

	public List<Tuple2<String, byte[]>> dequeue(String topic) {
		List<Tuple2<String, byte[]>> batch = new ArrayList<>();
		if (!contains(topic)) return batch;
		while (batch.size() < batchSize && queues.isEmpty())
			try {
				batch.add(decode(queues.get(topic).dequeue()));
			} catch (IOException e) {
				logger.error("Message dequeue/deserialize from local pool failure.", e);
			}
		return batch;
	}

	public KafkaQueue(String dataFolder, long batchSize) {
		this.dataFolder = dataFolder;
		this.batchSize = batchSize;
		this.closing = false;
		this.queues = new ConcurrentHashMap<>();
	}

	public long size(String topic) {
		return contains(topic) ? 0 : queues.get(topic).size();
	}

	public long size() {
		int c = 0;
		for (String topic : topics())
			c += size(topic);
		return c;
	}

	public Set<String> topics() {
		return queues.keySet();
	}

	public boolean contains(String topic) {
		return queues.containsKey(topic);
	}

	public IBigQueue queue(String topic) {
		if (contains(topic)) return queues.get(topic);
		else {
			IBigQueue q;
			try {
				q = new BigQueueImpl(dataFolder, topic);
			} catch (IOException e) {
				throw new RuntimeException("Local cache create failure.", e);
			}
			synchronized (queues) {
				queues.put(topic, q);
			}
			return q;
		}
	}

	public final void sleep() {
		try {
			Thread.sleep(WAIT_MS);
		} catch (InterruptedException e) {}
	}

	@Override
	public final void close() {
		closing = true;
		while (size() > 0)
			sleep();
		for (String topic : topics()) {
			queues.get(topic);
		}
	}

	private byte[] encode(String topic, byte[] message) {
		byte[] t = topic.getBytes(Charsets.UTF_8);
		int l = t.length;
		ByteArrayOutputStream bo = new ByteArrayOutputStream();
		try {
			bo.write(l);
			bo.write(t);
			bo.write(message.length);
			bo.write(message);
			return bo.toByteArray();
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			try {
				bo.close();
			} catch (IOException e) {}
		}
	}

	private Tuple2<String, byte[]> decode(byte[] data) {
		ByteArrayInputStream bo = new ByteArrayInputStream(data);
		try {
			int l = bo.read();
			byte[] t = new byte[l];
			if (bo.read(t) != l) throw new RuntimeException();
			String topic = new String(t, Charsets.UTF_8);
			l = bo.read();
			t = new byte[l];
			if (bo.read(t) != l) throw new RuntimeException();
			return new Tuple2<>(topic, t);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			try {
				bo.close();
			} catch (IOException e) {}
		}
	}
}
