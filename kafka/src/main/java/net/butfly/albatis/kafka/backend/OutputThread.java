package net.butfly.albatis.kafka.backend;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

@SuppressWarnings("deprecation")
public class OutputThread extends Thread {
	private Queue context;
	private Producer<byte[], byte[]> producer;

	public OutputThread(Queue context, Producer<byte[], byte[]> p) {
		super();
		this.context = context;
		this.producer = p;
	}

	@Override
	public void run() {
		while (true) {
			while (context.size() > 0)
				for (Message m : context.dequeue(null))
					producer.send(new KeyedMessage<byte[], byte[]>(m.getTopic(), m.getKey(), m.getMessage()));
			context.sleep();
		}
	}
}
