package net.butfly.albatis.kafka.backend;

import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import net.butfly.albacore.lambda.Task;

public class InputThread extends Thread {
	private KafkaStream<byte[], byte[]> stream;
	private Queue context;
	private int commitBatch;
	private Task committing;

	public InputThread(Queue context, KafkaStream<byte[], byte[]> stream, int commitBatch, Task committing) {
		super();
		this.context = context;
		this.stream = stream;
		this.commitBatch = commitBatch;
		this.committing = committing;
	}

	@Override
	public void run() {
		int c = 0;
		while (true) {
			for (MessageAndMetadata<byte[], byte[]> msgAndMetadata : stream) {
				context.enqueue(msgAndMetadata.topic(), msgAndMetadata.key(), msgAndMetadata.message());
				c++;
				if (c >= commitBatch) committing.call();
			}
			context.sleep();
		}
	}
}
