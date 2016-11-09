package net.butfly.albatis.dao;

import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class KafkaTest {
	public static void main(String[] args) {
		ConfigurableApplicationContext context = new ClassPathXmlApplicationContext("/net/butfly/albatis/kafka/test/spring/beans-test.xml");
		// KafkaDao dao = context.getBean("kafkaTestDao", KafkaDao.class);
		// User[] users = dao.select(User.class);
		// System.out.println(users.length);
		context.close();
	}
}
