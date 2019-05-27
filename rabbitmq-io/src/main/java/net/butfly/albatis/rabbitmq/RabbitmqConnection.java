package net.butfly.albatis.rabbitmq;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.RpcClient;

import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.DataConnection;

public class RabbitmqConnection extends DataConnection<RpcClient> {
	
	public Connection conn;
	
	public String exchange_name;
	
	//amqp ://username       :password           @hostName   :    portNumber/exchange
	protected RabbitmqConnection(URISpec  uri, String... supportedSchema) throws IOException {
		super(uri, supportedSchema);
		ConnectionFactory factory = new ConnectionFactory();
		try {
	        //设置RabbitMQ地址
			exchange_name = uri.getFile();
	        factory.setHost(uri.getHost());
	        factory.setUsername(uri.getUsername());
	        factory.setPassword(uri.getPassword());
	        factory.setPort(uri.getDefaultPort());
			conn = factory.newConnection();
		} catch (TimeoutException e) {
			e.printStackTrace();
		}
	}

	public RabbitmqConnection(URISpec uri) throws IOException {
		this(uri, "rabbitmq");
		ConnectionFactory factory = new ConnectionFactory();
		try {
			 //设置RabbitMQ地址
			exchange_name = uri.getFile();
	        factory.setHost(uri.getHost());
	        factory.setUsername(uri.getUsername());
	        factory.setPassword(uri.getPassword());
	        factory.setPort(uri.getDefaultPort());
			conn = factory.newConnection();
		} catch (TimeoutException e) {
			e.printStackTrace();
		}
	}
	
	
	@Override
	public RabbitmqInput input(String... tables) {
		try {
			return new RabbitmqInput("RabbitmqInput", conn, tables[0]);
		} catch (ConfigException e) {
			throw new IllegalArgumentException(e);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public RabbitmqOutput output(String... tables) throws IOException {
		try {
			return new RabbitmqOutput("RabbitmqOutput", conn, exchange_name, tables[0]);
		} catch (TimeoutException e) {
			e.printStackTrace();
		}
		return null;
	}

}