package net.butfly.albatis.kafka;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.base.Charsets;
import com.leansoft.bigqueue.BigQueueImpl;
import com.leansoft.bigqueue.IBigQueue;

import net.butfly.albacore.utils.async.Tasks;
import net.butfly.albacore.utils.logger.Logger;

class Queue implements Closeable {
	private static final Logger logger = Logger.getLogger(Queue.class);
	private final long poolSize;
	private Boolean closing;
	private final String dataFolder;
	private final Map<String, IBigQueue> queue;

	Queue(String dataFolder, long poolSize) {
		this.dataFolder = dataFolder;
		this.poolSize = poolSize;
		this.closing = false;
		this.queue = new ConcurrentHashMap<>();
	}

	void enqueue(Message... message) {
		if (closing) return;
		while (size() >= poolSize)
			Tasks.waitSleep(500, () -> logger.trace("Kafka pool full, sleeping for 500ms"));
		logger.trace("Kafka pool enqueuing: [" + message.length + "] messages.");
		for (Message m : message) {
			// BSONObject b = new BasicBSONDecoder().readObject(m.getMessage());
			// logger.warn(b.toMap().toString());
			IBigQueue q = queue(m.topic);
			try {
				q.enqueue(m.toBytes());
				q.peek();
			} catch (IOException e) {
				logger.error("Message enqueue/serialize to local pool failure.", e);
			}
		}
	}

	List<Message> dequeue(long batchSize, String... topic) {
		topic = topics(topic);
		List<Message> batch = new ArrayList<>();
		int prev;
		do {
			prev = batch.size();
			for (String t : topic) {
				IBigQueue q = queue.get(t);
				if (null == q || q.size() == 0) continue;
				try {
					byte[] m = q.dequeue();
					if (null != m) batch.add(new Message(m));
				} catch (IOException e) {
					logger.error("Message dequeue/deserialize from local pool failure.", e);
				}
			}
		} while (batch.size() < batchSize
				&& !(prev == batch.size() && !Tasks.waitSleep(500, () -> logger.trace("Kafka pool empty, sleeping for 500ms."))));
		logger.trace("Kafka pool dequeued: [" + batch.size() + "] messages.");
		return batch;
	}

	long size(String... topic) {
		long s = 0;
		for (String t : topics(topic))
			s += contains(t) ? 0 : queue.get(topic).size();
		return s;
	}

	String[] topics(String... topic) {
		return null == topic || topic.length == 0 ? queue.keySet().toArray(new String[0]) : topic;
	}

	boolean contains(String... topic) {
		for (String t : topics(topic))
			if (queue.containsKey(t)) return true;
		return false;
	}

	private IBigQueue queue(String topic) {
		if (contains(topic)) return queue.get(topic);
		else {
			IBigQueue q;
			try {
				logger.info("Off heap queue create at [" + dataFolder + "]");
				q = new BigQueueImpl(dataFolder, topic);
			} catch (IOException e) {
				throw new RuntimeException("Local cache create failure.", e);
			}
			synchronized (queue) {
				queue.put(topic, q);
			}
			return q;
		}
	}

	@Override
	public final void close() {
		synchronized (closing) {
			if (closing) return;
			closing = true;
			for (String t : queue.keySet()) {
				try {
					queue.get(t).close();
				} catch (IOException e) {
					logger.error("QueueBase on [" + t + "] close failure.", e);
				}
				queue.remove(t);
			}
			closing = false;
		}
	}

	static class Message implements Serializable {
		private static final long serialVersionUID = -8599938670114294267L;
		private String topic;
		private byte[] key;
		private byte[] message;

		Message(String topic, byte[] key, byte[] message) {
			super();
			this.topic = topic;
			this.key = key;
			this.message = message;
		}

		Message(byte[] data) {
			try (ByteArrayInputStream bo = new ByteArrayInputStream(data)) {
				topic = new String(read(bo), Charsets.UTF_8);
				key = read(bo);
				message = read(bo);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		String getTopic() {
			return topic;
		}

		byte[] getKey() {
			return key;
		}

		byte[] getMessage() {
			return message;
		}

		private byte[] toBytes() {
			try (ByteArrayOutputStream bo = new ByteArrayOutputStream()) {
				write(bo, topic.getBytes(Charsets.UTF_8));
				write(bo, key);
				write(bo, message);
				return bo.toByteArray();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		private static void write(ByteArrayOutputStream os, byte[] data) throws IOException {
			if (null == data) os.write(-1);
			else {
				int l = data.length;
				os.write(l & 0x000000FF);
				os.write((l >> 8) & 0x000000FF);
				os.write((l >> 16) & 0x000000FF);
				os.write((l >> 24) & 0x000000FF);
				if (data.length > 0) os.write(data);
			}
		}

		private static byte[] read(ByteArrayInputStream bo) throws IOException {
			int len = bo.read();
			len = (bo.read() << 8) | len;
			len = (bo.read() << 16) | len;
			len = (bo.read() << 24) | len;
			if (len < 0) return null;
			if (len == 0) return new byte[0];
			byte[] data = new byte[len];
			if (bo.read(data) != len) throw new RuntimeException();
			return data;
		}
	}

	boolean full(String... topic) {
		return size(topic) >= poolSize;
	}
}
