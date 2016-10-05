//package net.butfly.albatis.kafka;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//
//import com.leansoft.bigqueue.BigQueueImpl;
//import com.leansoft.bigqueue.IBigQueue;
//
//import net.butfly.albacore.io.MapQueueImpl;
//import net.butfly.albacore.io.OffHeapQueue;
//import net.butfly.albacore.utils.async.Concurrents;
//import net.butfly.albacore.utils.logger.Logger;
//
//class BytesQueue extends MapQueueImpl<byte[], OffHeapQueue> {
//	private static final long serialVersionUID = -6626377040690810087L;
//	private static final Logger logger = Logger.getLogger(BytesQueue.class);
//
//	public BytesQueue(String dataFolder, int capacity) {
//		super("kafka-queue", key -> new OffHeapQueue(key, dataFolder, capacity), capacity);
//	}
//
//	@Override
//	public boolean enqueue(byte[] e) {
//		return false;
//	}
//
//	@Override
//	public long enqueue(byte[]... message) {
//		while (size() >= capacity()) {
//			logger.debug("Kafka pool full...gc it");
//			gc();
//			Concurrents.waitSleep(5000);
//		}
//		for (Message m : message) {
//			IBigQueue q = queue(m.getTopic());
//			try {
//				byte[] data = m.toBytes();
//				q.enqueue(data);
//				stats.in(data.length);
//			} catch (IOException e) {
//				logger.error("Message enqueue/serialize to local pool failure.", e);
//			}
//		}
//	}
//
//	List<Message> dequeue(long batchSize, String... topic) {
//		if (!running()) return new ArrayList<>();
//		String[] topics = topics(topic);
//		while (topics.length == 0) {
//			sleepEmpty();
//			topics = topics(topic);
//		}
//		List<Message> batch = new ArrayList<>();
//		int prev;
//		do {
//			prev = batch.size();
//			for (String to : topics) {
//				IBigQueue q = queue.get(to);
//				if (null == q || q.size() == 0) continue;
//				try {
//					byte[] data = q.dequeue();
//					if (null != data) {
//						batch.add(new Message(data));
//						stats.out(data.length);
//					}
//				} catch (IOException e) {
//					logger.error("Message dequeue/deserialize from local pool failure.", e);
//				}
//			}
//		} while (batch.size() < batchSize && !(prev == batch.size() && !sleepEmpty()));
//		return batch;
//	}
//
//	long size(String... topic) {
//		long s = 0;
//		for (String t : topics(topic))
//			s += contains(t) ? queue.get(t).size() : 0;
//		return s;
//	}
//
//	String[] topics(String... topic) {
//		return null == topic || topic.length == 0 ? queue.keySet().toArray(new String[0]) : topic;
//	}
//
//	boolean contains(String... topic) {
//		for (String t : topics(topic))
//			if (queue.get(t) != null) return true;
//		return false;
//	}
//
//	private IBigQueue queue(String topic) {
//		if (contains(topic)) return queue.get(topic);
//		else {
//			IBigQueue q;
//			try {
//				logger.info("Off heap queue creating for topic [" + topic + "] at [" + dataFolder + "]");
//				q = new BigQueueImpl(dataFolder, topic);
//			} catch (IOException e) {
//				throw new RuntimeException("Local cache create failure.", e);
//			}
//			try {
//				q.gc();
//			} catch (IOException e) {
//				logger.error("Local queue GC failure", e);
//			}
//			synchronized (queue) {
//				queue.put(topic, q);
//			}
//			return q;
//		}
//	}
//
//	@Override
//	public final void close() {
//		super.close();
//		for (String t : queue.keySet()) {
//			try {
//				queue.get(t).close();
//			} catch (IOException e) {
//				logger.error("QueueBase on [" + t + "] close failure.", e);
//			}
//			queue.remove(t);
//		}
//	}
//
//	public void statsOnStep(long step) {
//		stats.step(step);
//	}
//
//	private boolean sleepEmpty() {
//		logger.debug("Kafka pool empty...gc it");
//		gc();
//		return Concurrents.waitSleep(1000);
//	}
//
//	private void gc() {
//		for (String topic : queue.keySet())
//			try {
//				queue.get(topic).gc();
//			} catch (IOException e) {
//				logger.error("Local queue GC failure", e);
//			}
//	}
//
//	@Override
//	protected net.butfly.albacore.io.AbstractQueue.Policy policy() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//}
