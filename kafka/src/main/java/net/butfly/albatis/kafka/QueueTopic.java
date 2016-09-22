package net.butfly.albatis.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.leansoft.bigqueue.BigQueueImpl;
import com.leansoft.bigqueue.IBigQueue;

import scala.Tuple2;

class QueueTopic extends Queue {
	private Map<String, IBigQueue> queues;

	public QueueTopic(String dataFolder, long batchSize) {
		this.dataFolder = dataFolder;
		this.batchSize = batchSize;
		this.closing = false;
		this.queues = new ConcurrentHashMap<>();
	}

	@Override
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

	@Override
	public long size(String topic) {
		return contains(topic) ? 0 : queues.get(topic).size();
	}

	@Override
	public long size() {
		int c = 0;
		for (String topic : topics())
			c += size(topic);
		return c;
	}

	@Override
	public Set<String> topics() {
		return queues.keySet();
	}

	@Override
	public boolean contains(String topic) {
		return queues.containsKey(topic);
	}

	@Override
	protected IBigQueue queue(String topic) {
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

	@Override
	public final void close() {
		super.close();
		for (String topic : topics())
			try {
				queues.get(topic).close();
			} catch (IOException e) {
				logger.error("Queue on [" + topic + "] close failure.", e);
			}
	}
}
