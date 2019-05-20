package net.butfly.albatis.rabbitmq;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.io.OutputBase;
import net.butfly.albatis.io.Rmap;

public class RabbitmqOutput extends OutputBase<Rmap> {
	private static final long serialVersionUID = -2376114954650957250L;
	private final Channel channel;
	private static final String exchange_name = "";
	private static final String routingKey = "";

	protected RabbitmqOutput(String name, Connection conn, String table) throws IOException, TimeoutException {
		super(name);
		 // 获取到连接以及mq通道
        channel = conn.createChannel();
        // 声明exchange
        channel.exchangeDeclare(exchange_name, "direct");
        channel.close();
	}


	@Override
	protected void enqsafe(Sdream<Rmap> messages) {
		List<Rmap> records = messages.list();
		records.forEach(r ->{
			try {
				channel.basicPublish(exchange_name, routingKey, null, r.get("data").toString().getBytes());
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
	}
}