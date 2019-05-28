package net.butfly.albatis.rabbitmq;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.Configs;
import net.butfly.albatis.io.OutputBase;
import net.butfly.albatis.io.Rmap;

public class RabbitmqOutput extends OutputBase<Rmap> {
	private static final long serialVersionUID = -2376114954650957250L;
	private static Channel channel;
	private static String exchange_name;
	private static String routing_Key;
	//private static String queue_name;

	protected RabbitmqOutput(String name, Connection conn, String exchange_name, String table) throws IOException, TimeoutException {
		super(name);
		//RabbitmqOutput.queue_name = table;
		RabbitmqOutput.exchange_name = exchange_name;
		RabbitmqOutput.routing_Key = table;
		 // 获取到连接以及mq通道
        channel = conn.createChannel();
        // 声明exchange
        channel.exchangeDeclare(exchange_name, Configs.gets("albatis.rabbitmq.exchange.type", "direct"));
     //   channel.queueDeclare(queue, durable, exclusive, autoDelete, arguments)
       // channel.queueBind(queue, exchange, routingKey)
        channel.close();
	}
	
	protected RabbitmqOutput(String name, RabbitmqConnection conn, String table) throws IOException, TimeoutException {
		super(name);
		conn.output(table);
	}
	
	

	@Override
	protected void enqsafe(Sdream<Rmap> messages) {
		List<Rmap> records = messages.list();
		records.forEach(r ->{
			try {
				channel.basicPublish(exchange_name, routing_Key, null, r.get("km").toString().getBytes());
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
	}
}