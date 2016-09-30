package net.butfly.albatis.kafka;

import java.util.ArrayList;
import java.util.List;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.kafka.Queue.Message;

class OutputThread extends Thread {
	private final static Logger logger = Logger.getLogger(OutputThread.class);
	private Queue context;
	private Producer<byte[], byte[]> producer;
	private long batchSize;

	OutputThread(Queue context, Producer<byte[], byte[]> p, long batchSize) {
		super();
		this.context = context;
		this.producer = p;
		this.batchSize = batchSize;
	}

	@Override
	public void run() {
		while (true) {
			List<Message> msgs = context.dequeue(batchSize);
			List<KeyedMessage<byte[], byte[]>> l = new ArrayList<>(msgs.size());
			for (Message m : msgs)
				l.add(new KeyedMessage<byte[], byte[]>(m.getTopic(), m.getKey(), m.getMessage()));
			producer.send(l);
			logger.trace(() -> "Kafka service sent (amount: [" + msgs.size() + "]).");
		}
	}
}
