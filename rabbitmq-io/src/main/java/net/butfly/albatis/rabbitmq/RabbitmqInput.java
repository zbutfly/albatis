package net.butfly.albatis.rabbitmq;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import net.butfly.albatis.io.OddInput;
import net.butfly.albatis.io.Rmap;

public class RabbitmqInput extends net.butfly.albacore.base.Namedly implements OddInput<Rmap> {
	private static final long serialVersionUID = 2242853649760090074L;
	//private static final Logger logger = Logger.getLogger(DataHubInput.class);
	
	private final ConcurrentLinkedQueue<String> consumer;
	private static final String charset = "UTF-8";
	private String queue_name;
	
	{
		consumer = new ConcurrentLinkedQueue<>();
	}
	
	protected RabbitmqInput(String name, Connection conn, String queuename) throws IOException {
		super(name);
		this.queue_name = queuename;
		// 创建一个通道
		Channel channel = conn.createChannel();
		// 声明要关注的队列
		channel.queueDeclare(queuename, false, false, true, null);
		// DefaultConsumer类实现了Consumer接口，通过传入一个频道，
		// 告诉服务器我们需要那个频道的消息，如果频道中有消息，就会执行回调函数handleDelivery
		Consumer c = new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
					byte[] body) throws IOException {
				consumer.add(new String(body, charset));
			}
		};
		// 自动回复队列应答 -- RabbitMQ中的消息确认机制
		channel.basicConsume(queuename, true, c);
		closing(this::close);
	}
	
	
	public RabbitmqInput(String name, RabbitmqConnection conn, String table) {
		super(name);
		this.queue_name = table;
		conn.input(table);
	}
	

	@SuppressWarnings("deprecation")
	@Override
	public Rmap dequeue(){
		while (opened()) {
			String c = consumer.poll();
			return new Rmap(queue_name, null,"km", c);
		}
		return null;
	}
}
