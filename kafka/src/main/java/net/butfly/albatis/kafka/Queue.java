package net.butfly.albatis.kafka;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.leansoft.bigqueue.IBigQueue;

import scala.Tuple2;

abstract class Queue implements AutoCloseable {
	protected static final Logger logger = LoggerFactory.getLogger(Queue.class);
	protected static final long WAIT_MS = 1000;
	protected long batchSize;
	protected boolean closing;
	protected String dataFolder;

	public void enqueue(String topic, byte[] message) {
		if (!closing) try {
			queue(topic).enqueue(encode(topic, message));
		} catch (IOException e) {
			logger.error("Message enqueue/serialize to local pool failure.", e);
		}
	}

	public abstract List<Tuple2<String, byte[]>> dequeue(String topic);

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
		// try {
		// Thread.sleep(WAIT_MS);
		// } catch (InterruptedException e) {}
	}

	protected abstract IBigQueue queue(String topic);

	protected final byte[] encode(String topic, byte[] message) {
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

	protected final Tuple2<String, byte[]> decode(byte[] data) {
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
