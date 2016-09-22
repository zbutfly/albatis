package net.butfly.albatis.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import scala.Tuple2;

@SuppressWarnings("deprecation")
class ProducerThread extends Thread {
	private Queue context;
	private Producer<String, byte[]> producer;

	public ProducerThread(Queue context, Producer<String, byte[]> p) {
		super();
		this.context = context;
		this.producer = p;
	}

	@Override
	public void run() {
		while (true) {
			while (context.size() > 0)
				for (Tuple2<String, byte[]> t : context.dequeue(null))
					producer.send(new KeyedMessage<String, byte[]>(t._1, t._2));
			context.sleep();
		}
	}
}
