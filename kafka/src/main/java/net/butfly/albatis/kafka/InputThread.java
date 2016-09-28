package net.butfly.albatis.kafka;

import java.util.ArrayList;
import java.util.List;

import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import net.butfly.albacore.lambda.Task;
import net.butfly.albatis.kafka.Queue.Message;

class InputThread extends Thread {
	private KafkaStream<byte[], byte[]> stream;
	private Queue context;
	private long batchSize;
	private Task committing;

	InputThread(Queue context, KafkaStream<byte[], byte[]> stream, long batciSize, Task committing) {
		super();
		this.context = context;
		this.stream = stream;
		this.batchSize = batciSize;
		this.committing = committing;
	}

	@Override
	public void run() {
		long c = 0;
		while (true) {
			List<Message> messages = new ArrayList<>();
			for (MessageAndMetadata<byte[], byte[]> msgAndMetadata : stream) {
				messages.add(new Message(msgAndMetadata.topic(), msgAndMetadata.key(), msgAndMetadata.message()));
				if (messages.size() > batchSize) break;
			}
			long s = messages.size();
			c += s;
			context.enqueue(messages.toArray(new Message[messages.size()]));
			if (c > batchSize) {
				committing.call();
				c = 0;
			}
			if (s < batchSize) context.sleep();
		}
	}

}
