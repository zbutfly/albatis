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

import net.butfly.albacore.utils.logger.Logger;

import com.google.common.base.Charsets;
import com.leansoft.bigqueue.BigQueueImpl;
import com.leansoft.bigqueue.IBigQueue;

class Queue implements Closeable {
	protected static final Logger logger = Logger.getLogger(Queue.class);
	protected static final long WAIT_MS = 1000;
	protected long poolSize;
	protected Boolean closing;
	protected String dataFolder;
	private Map<String, IBigQueue> queue;

	Queue(String dataFolder, long poolSize) {
		this.dataFolder = dataFolder;
		this.poolSize = poolSize;
		this.closing = false;
		this.queue = new ConcurrentHashMap<>();
	}

	void enqueue(Message... message) {
		if (closing) return;
		while (size() >= poolSize)
			sleep();
		for (Message m : message) {
			IBigQueue q = queue(m.topic);
			try {
				q.enqueue(m.toBytes());
			} catch (IOException e) {
				logger.error("Message enqueue/serialize to local pool failure.", e);
			}
		}
	}

	List<Message> dequeue(long batchSize, String... topic) {
		topic = topics(topic);
		List<Message> batch = new ArrayList<>();
		long remain;
		do {
			remain = 0;
			for (String t : topic) {
				IBigQueue q = queue.get(t);
				if (null == q || q.size() == 0) continue;
				try {
					batch.add(new Message(q.dequeue()));
				} catch (IOException e) {
					logger.error("Message dequeue/deserialize from local pool failure.", e);
				}
				remain += q.size();
				logger.trace("Queue dequeue: [" + batch.size() + "] messages, remain: [" + remain + "] messages.");
			}
		} while (batch.size() < batchSize && !(remain <= 0 && !sleep()));
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

	final boolean sleep() {
		Thread.yield();
		return true;
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
				os.write(data.length);
				if (data.length > 0) os.write(data);
			}
		}

		private static byte[] read(ByteArrayInputStream bo) throws IOException {
			int len = bo.read();
			if (len == -1) return null;
			if (len == 0) return new byte[0];
			byte[] data = new byte[len];
			if (bo.read(data) != len) throw new RuntimeException();
			return data;
		}
	}

}
