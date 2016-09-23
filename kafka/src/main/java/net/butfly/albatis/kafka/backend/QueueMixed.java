package net.butfly.albatis.kafka.backend;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.leansoft.bigqueue.BigQueueImpl;
import com.leansoft.bigqueue.IBigQueue;

public class QueueMixed extends Queue {
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
