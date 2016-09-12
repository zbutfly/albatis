package net.butfly.albatis.dao;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class KafkaTest {
	public static void main(String[] args) {
		ApplicationContext context = new ClassPathXmlApplicationContext("/net/butfly/albatis/kafka/test/spring/beans-test.xml");
		KafkaDao dao = context.getBean("kafkaTestDao", KafkaDao.class);
		User[] users = dao.select(User.class);
	}
}
