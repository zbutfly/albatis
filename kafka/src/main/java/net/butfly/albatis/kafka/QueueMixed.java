package net.butfly.albatis.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.leansoft.bigqueue.BigQueueImpl;
import com.leansoft.bigqueue.IBigQueue;

import scala.Tuple2;

class QueueMixed extends Queue {
	private IBigQueue queue;

	public QueueMixed(String dataFolder, long batchSize) {
		this.dataFolder = dataFolder;
		this.batchSize = batchSize;
		this.closing = false;
		try {
			this.queue = new BigQueueImpl(dataFolder, "_QWERT_MIX_");
		} catch (IOException e) {
			throw new RuntimeException("Local cache create failure.", e);
		}
	}

	@Override
	public List<Tuple2<String, byte[]>> dequeue(String topic) {
		List<Tuple2<String, byte[]>> batch = new ArrayList<>();
		if (!contains(topic)) return batch;
		while (batch.size() < batchSize && queue.isEmpty())
			try {
				batch.add(decode(queue.dequeue()));
			} catch (IOException e) {
				logger.error("Message dequeue/deserialize from local pool failure.", e);
			}
		return batch;
	}

	@Override
	public long size(String topic) {
		return size();
	}

	@Override
	public long size() {
		return queue.size();
	}

	@Override
	public Set<String> topics() {
		return new HashSet<>();
	}

	@Override
	public boolean contains(String topic) {
		return true;
	}

	@Override
	protected IBigQueue queue(String topic) {
		return queue;
	}

	@Override
	public final void close() {
		super.close();
		try {
			queue.close();
		} catch (IOException e) {
			logger.error("Queue close failure.", e);
		}
	}

}
