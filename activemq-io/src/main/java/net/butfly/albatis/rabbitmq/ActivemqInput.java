package net.butfly.albatis.rabbitmq;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.io.OddInput;
import net.butfly.albatis.io.Rmap;

public class ActivemqInput extends net.butfly.albacore.base.Namedly implements OddInput<Rmap> {
	private static final long serialVersionUID = 2242853649760090074L;

	private final BlockingQueue<MessageConsumer> consumers;
	private String queuename;

	{
		consumers = new LinkedBlockingQueue<>();
	}

	protected ActivemqInput(String name, URISpec uri, String... queuename) throws IOException {
		super(name);
		this.queuename = queuename[0];
		try {
			ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(uri.getUsername(), uri.getPassword(),
					"tcp://" + uri.getHost() + ":" + uri.getDefaultPort());
			Connection conn = connectionFactory.createConnection();
			conn.start();
			Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			// message to
			Destination destination = session.createQueue(queuename[0]);
			// message consumer
			MessageConsumer consumer = session.createConsumer(destination);
			consumers.add(consumer);
			closing(this::close);
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("deprecation")
	@Override
	public Rmap dequeue() {
		while (opened()) {
			MessageConsumer consumer = consumers.poll();
			if (null != consumer) {
				try {
					TextMessage message;
					message = (TextMessage) consumer.receive();
					consumers.add(consumer);
					if (message != null) {
						return new Rmap(queuename, null,"km", message);
					} else {
						return null;
					}
				} catch (JMSException e) {
					e.printStackTrace();
				}
			}
		}
		return null;
	}
}
