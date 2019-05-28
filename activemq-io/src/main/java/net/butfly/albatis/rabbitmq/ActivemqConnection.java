package net.butfly.albatis.rabbitmq;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import javax.jms.Connection;

import org.apache.activemq.ActiveMQConnection;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.DataConnection;

public class ActivemqConnection extends DataConnection<ActiveMQConnection> {
	
	public Connection conn;
	
	public String exchange_name;
	//activemq://username:password@host:port
	protected ActivemqConnection(URISpec uri, String... supportedSchema) throws IOException {
		super(uri, supportedSchema);
	}

	public ActivemqConnection(URISpec uri) throws IOException {
		this(uri, "activemq");
	}
	
	
	@Override
	public ActivemqInput input(String... tables) {
		try {
			return new ActivemqInput("ActivemqInput", uri, tables[0]);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public ActivemqOutput output(String... tables) throws IOException {
		try {
			return new ActivemqOutput("ActivemqOutput", uri,tables[0]);
		} catch (TimeoutException e) {
			e.printStackTrace();
		}
		return null;
	}

}