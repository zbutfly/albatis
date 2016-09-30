package net.butfly.albatis.kafka;

import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import net.butfly.albacore.lambda.Task;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.kafka.Queue.Message;

class InputThread extends Thread {
	private final static Logger logger = Logger.getLogger(InputThread.class);
	private final KafkaStream<byte[], byte[]> stream;
	private final Queue context;
	private final long batchSize;
	private final Task committing;
	private final String topic;

	InputThread(Queue context, String topic, KafkaStream<byte[], byte[]> stream, long batchSize, Task committing) {
		super();
		this.topic = topic;
		this.context = context;
		this.stream = stream;
		this.batchSize = batchSize;
		this.committing = committing;
	}

	@Override
	public void run() {
		long c = 0;
		while (true) {
			for (MessageAndMetadata<byte[], byte[]> msgAndMetadata : stream) {
				Message m = new Message(msgAndMetadata.topic(), msgAndMetadata.key(), msgAndMetadata.message());
				context.enqueue(m);
				if (++c > batchSize) {
					committing.call();
					if (logger.isTraceEnabled()) logger.trace(c + " Kafka messages on Topic@[" + topic + "] fetched and committed.");
					c = 0;
				}
			}
		}
	}
}
