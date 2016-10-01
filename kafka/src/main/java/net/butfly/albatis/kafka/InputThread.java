package net.butfly.albatis.kafka;

import kafka.consumer.ConsumerIterator;
import kafka.message.MessageAndMetadata;
import net.butfly.albacore.lambda.Runnable;
import net.butfly.albacore.utils.async.Concurrents;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.kafka.Queue.Message;

class InputThread extends Thread {
	private final static Logger logger = Logger.getLogger(InputThread.class);
	private final ConsumerIterator<byte[], byte[]> iter;
	private final Queue context;
	private final long batchSize;
	private final Runnable committing;
	private final String topic;

	InputThread(Queue context, String topic, ConsumerIterator<byte[], byte[]> iter, long batchSize, Runnable committing) {
		super();
		this.topic = topic;
		this.context = context;
		this.iter = iter;
		this.batchSize = batchSize;
		this.committing = committing;
	}

	@Override
	public void run() {
		long c = 0;
		while (true) {
			if (!iter.hasNext()) Concurrents.waitSleep(500, logger, "Kafka service empty (topic: [" + topic + "]).");
			else {
				MessageAndMetadata<byte[], byte[]> meta = iter.next();
				if (++c > batchSize || iter.hasNext()) {
					committing.run();
					final long cc = c;
					logger.trace(() -> "Kafka service committed (topic: [" + topic + "], amount: [" + cc + "]).");
					c = 0;
				}
				Message m = new Message(meta.topic(), meta.key(), meta.message());
				context.enqueue(m);
			}
		}
	}
}
