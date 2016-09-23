package net.butfly.albatis.kafka.backend;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.leansoft.bigqueue.IBigQueue;

public abstract class Queue implements Closeable {
	protected static final Logger logger = LoggerFactory.getLogger(Queue.class);
	protected static final long WAIT_MS = 1000;
	protected long batchSize;
	protected boolean closing;
	protected String dataFolder;

	public void enqueue(String topic, byte[] key, byte[] message) {
		if (!closing) try {
			queue(topic).enqueue(new Message(topic, key, message).toBytes());
		} catch (IOException e) {
			logger.error("Message enqueue/serialize to local pool failure.", e);
		}
	}

	public List<Message> dequeue(String topic) {
		List<Message> batch = new ArrayList<>();
		if (!contains(topic)) return batch;
		while (batch.size() < batchSize && size(topic) > 0)
			try {
				batch.add(new Message(queue(topic).dequeue()));
			} catch (IOException e) {
				logger.error("Message dequeue/deserialize from local pool failure.", e);
			}
		return batch;
	}

	public abstract long size();

	public abstract long size(String topic);

	public abstract Set<String> topics();

	public abstract boolean contains(String topic);

	public void close() {
		closing = true;
		while (size() > 0)
			sleep();
	}

	public final void sleep() {
		Thread.yield();
	}

	protected abstract IBigQueue queue(String topic);

}
