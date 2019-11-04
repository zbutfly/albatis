package net.butfly.albatis.activemq;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.OutputBase;
import net.butfly.albatis.io.Rmap;

public class ActivemqOutput extends OutputBase<Rmap> {
	private static final long serialVersionUID = -2376114954650957250L;
	
	private final ActivemqConnection connection;
	private Session session;
	private String[] tables;
	private final String mode;
	private Map<String, MessageProducer> producers = Maps.of();

	public ActivemqOutput(String name, ActivemqConnection connection, String... tables) throws IOException {
		super(name);
		this.tables = tables;
		this.connection = connection;
		mode = "topic".equals(connection.mode()) ? "topic" : "queue";
		try {
			this.session = connection.client.createSession(false, Session.AUTO_ACKNOWLEDGE);
//			for (String table : tables) {
//				Destination destination = "topic".equals(mode) ? session.createTopic(table) : session.createQueue(table);
//				// message consumer
//				MessageConsumer consumer = session.createConsumer(destination);
//				consumers.add(consumer);
//			}
			closing(this::close);
		} catch (JMSException e) {
			throw new RuntimeException("Create session failed", e);
		}
	}
	
	@Override
	protected void enqsafe(Sdream<Rmap> messages) {
		List<Rmap> records = messages.list();
		records.forEach(r ->{
			MessageProducer producer = producers.computeIfAbsent(r.table().name, k -> {
				Destination destination;
				try {
					destination = "topic".equals(mode) ? session.createTopic(r.table().name) : session.createQueue(r.table().name);
					MessageProducer p = session.createProducer(destination);
					if ("NON_PERSISTENT".equals(connection.uri().getParameter("deliveryMode")))
						p.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
					return p;
				} catch (JMSException e) {
					throw new RuntimeException("Create producer error!", e);
				}
			});
			r.forEach((k, body) -> {
				if (null != body) {
					try {
						TextMessage m = session.createTextMessage((String) body);
						producer.send(m);
//						session.commit();
					} catch (JMSException e) {
						e.printStackTrace();
					}
				}
			});
		});
	}

	@Override
	public void close() {
		try {
			for (MessageProducer producer : producers.values()) {
				producer.close();
				session.close();
			}
		} catch (JMSException e) {
		}
	}
}
