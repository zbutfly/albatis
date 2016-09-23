package net.butfly.albatis.kafka.backend;

import java.util.ArrayList;
import java.util.List;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import net.butfly.albatis.kafka.backend.Queue.Message;

@SuppressWarnings("deprecation")
public class OutputThread extends Thread {
	private Queue context;
	private Producer<byte[], byte[]> producer;
	private long batchSize;

	public OutputThread(Queue context, Producer<byte[], byte[]> p, long batchSize) {
		super();
		this.context = context;
		this.producer = p;
		this.batchSize = batchSize;
	}

	@Override
	public void run() {
		while (true) {
			while (context.size() > 0) {
				List<KeyedMessage<byte[], byte[]>> l = new ArrayList<>();
				for (Message m : context.dequeue(batchSize))
					l.add(new KeyedMessage<byte[], byte[]>(m.getTopic(), m.getKey(), m.getMessage()));
				producer.send(l);
			}
			context.sleep();
		}
	}
}
