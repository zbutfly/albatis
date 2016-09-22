package net.butfly.albatis.kafka;

import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

class ConsumerThread extends Thread {
	private KafkaStream<byte[], byte[]> stream;
	private Queue context;

	public ConsumerThread(Queue context) {
		super();
		this.context = context;
	}

	public void setStream(KafkaStream<byte[], byte[]> stream) {
		this.stream = stream;
	}

	@Override
	public void run() {
		if (stream != null) {
			for (MessageAndMetadata<byte[], byte[]> msgAndMetadata : stream) {
				String topic = msgAndMetadata.topic();
				byte[] message = msgAndMetadata.message();
				// waiting for full status
				context.sleep();
				context.enqueue(topic, message);
			}
		}
	}
}
