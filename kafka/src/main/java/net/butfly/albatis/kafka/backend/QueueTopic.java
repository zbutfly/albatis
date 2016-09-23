//package net.butfly.albatis.kafka.backend;
//
//import java.io.IOException;
//import java.util.Map;
//import java.util.Set;
//import java.util.concurrent.ConcurrentHashMap;
//
//import com.leansoft.bigqueue.BigQueueImpl;
//import com.leansoft.bigqueue.IBigQueue;
//
//public class QueueTopic extends QueueBase {
//	private Map<String, IBigQueue> queue;
//
//	public QueueTopic(String dataFolder, long batchSize) {
//		this.dataFolder = dataFolder;
//		this.size = batchSize;
//		this.closing = false;
//		this.queue = new ConcurrentHashMap<>();
//	}
//
//	@Override
//	public long poolSize(String topic) {
//		return contains(topic) ? 0 : queue.get(topic).size();
//	}
//
//	@Override
//	public long poolSize() {
//		int c = 0;
//		for (String topic : topics())
//			c += poolSize(topic);
//		return c;
//	}
//
//	@Override
//	public Set<String> topics() {
//		return queue.keySet();
//	}
//
//	@Override
//	public boolean contains(String topic) {
//		return queue.containsKey(topic);
//	}
//
//	@Override
//	protected IBigQueue queue(String topic) {
//		if (contains(topic)) return queue.get(topic);
//		else {
//			IBigQueue q;
//			try {
//				q = new BigQueueImpl(dataFolder, topic);
//			} catch (IOException e) {
//				throw new RuntimeException("Local cache create failure.", e);
//			}
//			synchronized (queue) {
//				queue.put(topic, q);
//			}
//			return q;
//		}
//	}
//
//	public final void close(String topic) {
//		try {
//			queue.get(topic).close();
//		} catch (IOException e) {
//			logger.error("QueueBase on [" + topic + "] close failure.", e);
//		}
//		queue.remove(topic);
//	}
//
//	@Override
//	public final void close() {
//		super.close();
//		for (String topic : topics())
//			close(topic);
//	}
//}
