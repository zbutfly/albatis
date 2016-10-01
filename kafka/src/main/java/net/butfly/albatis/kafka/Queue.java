package net.butfly.albatis.kafka;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongArray;

import com.google.common.base.Charsets;
import com.leansoft.bigqueue.BigQueueImpl;
import com.leansoft.bigqueue.IBigQueue;

import net.butfly.albacore.utils.async.Concurrents;
import net.butfly.albacore.utils.logger.Logger;

class Queue implements Closeable {
	private static final Logger logger = Logger.getLogger(Queue.class);
	private static final Logger slogger = Logger.getLogger("net.butfly.albatis.kafka.Queue.Staticstics");
	private final long poolSize;
	private AtomicBoolean closed;
	private final String dataFolder;
	private final Map<String, IBigQueue> queue;

	/* inStats: inMessages, inBytes, outMessages, outBytes, inTime, outTime */
	private final AtomicLongArray inStats;
	private final AtomicLongArray outStats;
	private long statsMsgs = 0;

	Queue(String dataFolder, long poolSize) {
		this.closed = new AtomicBoolean(false);
		long now = new Date().getTime();
		this.inStats = new AtomicLongArray(new long[] { 0, 0, now });
		this.outStats = new AtomicLongArray(new long[] { 0, 0, now });
		this.dataFolder = dataFolder;
		this.poolSize = poolSize;
		this.queue = new ConcurrentHashMap<>();
	}

	void enqueue(Message... message) {
		if (closed.get()) return;
		while (size() >= poolSize)
			sleepFull();

		for (Message m : message) {
			IBigQueue q = queue(m.topic);
			try {
				byte[] data = m.toBytes();
				q.enqueue(data);
				if (statsMsgs > 0) slogger.trace(() -> stats(this.inStats, "enququed", data.length, q.size()));
			} catch (IOException e) {
				logger.error("Message enqueue/serialize to local pool failure.", e);
			}
		}
	}

	List<Message> dequeue(long batchSize, String... topic) {
		if (closed.get()) return new ArrayList<>();
		String[] topics = topics(topic);
		while (topics.length == 0) {
			sleepEmpty();
			topics = topics(topic);
		}
		List<Message> batch = new ArrayList<>();
		int prev;
		do {
			prev = batch.size();
			for (String to : topics) {
				IBigQueue q = queue.get(to);
				if (null == q || q.size() == 0) continue;
				try {
					byte[] data = q.dequeue();
					if (null != data) {
						batch.add(new Message(data));
						if (statsMsgs > 0) slogger.trace(() -> stats(this.outStats, "deququed", data.length, q.size()));
					}
				} catch (IOException e) {
					logger.error("Message dequeue/deserialize from local pool failure.", e);
				}
			}
		} while (batch.size() < batchSize && !(prev == batch.size() && !sleepEmpty()));
		return batch;
	}

	public void stats(long statsMsgs) {
		this.statsMsgs = statsMsgs;
	}

	long size(String... topic) {
		long s = 0;
		for (String t : topics(topic))
			s += contains(t) ? queue.get(t).size() : 0;
		return s;
	}

	String[] topics(String... topic) {
		return null == topic || topic.length == 0 ? queue.keySet().toArray(new String[0]) : topic;
	}

	boolean contains(String... topic) {
		for (String t : topics(topic))
			if (queue.get(t) != null) return true;
		return false;
	}

	private IBigQueue queue(String topic) {
		if (contains(topic)) return queue.get(topic);
		else {
			IBigQueue q;
			try {
				logger.info("Off heap queue creating for topic [" + topic + "] at [" + dataFolder + "]");
				q = new BigQueueImpl(dataFolder, topic);
			} catch (IOException e) {
				throw new RuntimeException("Local cache create failure.", e);
			}
			try {
				q.gc();
			} catch (IOException e) {
				logger.error("Local queue GC failure", e);
			}
			synchronized (queue) {
				queue.put(topic, q);
			}
			return q;
		}
	}

	@Override
	public final void close() {
		closed.set(true);
		for (String t : queue.keySet()) {
			try {
				queue.get(t).close();
			} catch (IOException e) {
				logger.error("QueueBase on [" + t + "] close failure.", e);
			}
			queue.remove(t);
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

	private static double K = 1024;
	private static double M = K * K;
	private static double G = M * K;
	private static double T = G * K;
	private static DecimalFormat f = new DecimalFormat("#.##");

	private String stats(AtomicLongArray stats, String act, int bytes, long queueSize) {
		long c = stats.incrementAndGet(0);
		long b = stats.addAndGet(1, bytes);
		if (c % statsMsgs != 0) return null;

		long t = new Date().getTime();
		t = (t - stats.getAndSet(2, t));

		String bb;
		if (b > T * 0.8) bb = f.format(b / T) + "TB";
		else if (b > G * 0.8) bb = f.format(b / G) + "GB";
		else if (b > M * 0.8) bb = f.format(b / M) + "MB";
		else if (b > K * 0.8) bb = f.format(b / K) + "KB";
		else bb = f.format(b) + "Byte";

		return "Kafka data pool " + act + " total: " + c + " messages, " + bb + "; \n\tthis step (" + this.statsMsgs + " messages) spent "
				+ f.format(t / 1000.0) + "seconds; \n\tcurrent messages: " + queueSize + ".";
	}

	private void sleepFull() {
		logger.debug("Kafka pool full...gc it");
		gc();
		Concurrents.waitSleep(5000);
	}

	private boolean sleepEmpty() {
		logger.debug("Kafka pool empty...gc it");
		gc();
		return Concurrents.waitSleep(1000);
	}

	private void gc() {
		for (String topic : queue.keySet())
			try {
				queue.get(topic).gc();
			} catch (IOException e) {
				logger.error("Local queue GC failure", e);
			}
	}
}
