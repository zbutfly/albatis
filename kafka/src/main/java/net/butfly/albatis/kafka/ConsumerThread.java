package net.butfly.albatis.kafka;

import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

class ConsumerThread extends Thread {
	private KafkaStream<byte[], byte[]> stream;
	private Queue context;

	public ConsumerThread(Queue context, KafkaStream<byte[], byte[]> stream) {
		super();
		this.context = context;
		this.stream = stream;
	}

	@Override
	public void run() {
		while (true) {
			for (MessageAndMetadata<byte[], byte[]> msgAndMetadata : stream) {
				String topic = msgAndMetadata.topic();
				byte[] message = msgAndMetadata.message();
				context.enqueue(topic, message);
			}
			context.sleep();
		}
	}
}
