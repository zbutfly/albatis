package net.butfly.albatis.rabbitmq;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.io.OutputBase;
import net.butfly.albatis.io.Rmap;

public class ActivemqOutput extends OutputBase<Rmap> {
	private static final long serialVersionUID = -2376114954650957250L;
	
	private MessageProducer producer;

	protected ActivemqOutput(String name, URISpec uri, String... queuename) throws IOException, TimeoutException {
		super(name);
		try {
			ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(uri.getUsername(), uri.getPassword(),
					"tcp://" + uri.getHost() + ":" + uri.getDefaultPort());
			Connection conn;
			conn = connectionFactory.createConnection();
			conn.start();
			Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			// message to
			Destination destination = session.createQueue(queuename[0]);
			producer = session.createProducer(destination);
			// 设置不持久化，此处学习，实际根据项目决定
			producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	protected void enqsafe(Sdream<Rmap> messages) {
		List<Rmap> records = messages.list();
		records.forEach(r ->{
			try {
				producer.send((TextMessage)(r.get("km")));
			} catch (JMSException e) {
				e.printStackTrace();
			}
		});
	}
}