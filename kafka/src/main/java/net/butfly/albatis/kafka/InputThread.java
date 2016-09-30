package net.butfly.albatis.kafka;

import kafka.consumer.ConsumerIterator;
import kafka.message.MessageAndMetadata;
import net.butfly.albacore.lambda.Task;
import net.butfly.albacore.utils.async.Tasks;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.kafka.Queue.Message;

class InputThread extends Thread {
	private final static Logger logger = Logger.getLogger(InputThread.class);
	private final ConsumerIterator<byte[], byte[]> iter;
	private final Queue context;
	private final long batchSize;
	private final Task committing;
	private final String topic;

	InputThread(Queue context, String topic, ConsumerIterator<byte[], byte[]> iter, long batchSize, Task committing) {
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
			MessageAndMetadata<byte[], byte[]> meta = iter.next();
			if (null == meta) Tasks.waitSleep(500,
					() -> Tasks.waitSleep(500, () -> logger.trace("Kafka service empty (topic: [" + topic + "]), sleeping for 500ms")));
			else {
				if (++c > batchSize || iter.hasNext()) {
					committing.call();
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
