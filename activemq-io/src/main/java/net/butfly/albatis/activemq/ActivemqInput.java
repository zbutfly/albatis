package net.butfly.albatis.activemq;

import java.io.IOException;
import java.util.Enumeration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.io.OddInput;
import net.butfly.albatis.io.Rmap;

public class ActivemqInput extends net.butfly.albacore.base.Namedly implements OddInput<Rmap> {
	private static final long serialVersionUID = 2242853649760090074L;

	private final BlockingQueue<MessageConsumer> consumers = new LinkedBlockingQueue<>();
	private final ActivemqConnection connection;
	private Session session;
	private String[] tables;
	private final String mode;

	public ActivemqInput(String name, ActivemqConnection connection, String... tables) throws IOException {
		super(name);
		this.tables = tables;
		this.connection = connection;
		mode = "topic".equals(connection.mode()) ? "topic" : "queue";
		try {
			this.session = connection.client.createSession(false, Session.AUTO_ACKNOWLEDGE);
			for (String table : tables) {
				Destination destination = "topic".equals(mode) ? session.createTopic(table) : session.createQueue(table);
				// message consumer
				MessageConsumer consumer = session.createConsumer(destination);
				consumers.add(consumer);
			}
			closing(this::close);
		} catch (JMSException e) {
			throw new RuntimeException("Create session failed", e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public Rmap dequeue() {
		while (opened()) {
			MessageConsumer consumer = consumers.poll();
			if (null != consumer) {
				TextMessage message = null;
				String table = null;
				Enumeration<String> enums = null;
				try {
					message = (TextMessage) consumer.receive();
					table = message.getJMSDestination().toString().split("://")[1];
					enums = message.getPropertyNames();
				} catch (JMSException e) {
					logger().error("Receive message error!", e);
					return null;
				} finally {
					consumers.add(consumer);
				}
				try {
					Rmap map = new Rmap(new Qualifier(table));
					map.put("acm", message.getText());
//					if (enums != null && enums.hasMoreElements()) {
//						Object value = null;
//						while (enums.hasMoreElements()) {
//							String key = enums.nextElement();
//							value = message.getObjectProperty(key);
//							if (null != value) map.put(key, value);
//						}
//					}
					return map;
				} catch (JMSException e) {
					logger().error("fetch data error!", e);
				}
			}
			return null;
		}
		return null;
	}

	@Override
	public void close() {
		MessageConsumer consumer = null;
		try {
			while ((consumer = consumers.poll()) != null) {
				consumer.close();
			}
			session.close();
		} catch (JMSException e) {
		}
	}
}
