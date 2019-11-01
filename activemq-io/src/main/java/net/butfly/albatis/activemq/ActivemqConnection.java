package net.butfly.albatis.activemq;

import java.io.IOException;
import java.util.List;

import javax.jms.Connection;
import javax.jms.JMSException;

import org.apache.activemq.ActiveMQConnectionFactory;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.DataConnection;
import net.butfly.albatis.ddl.TableDesc;

public class ActivemqConnection extends DataConnection<Connection> {
	
	public String exchange_name;
	private final URISpec uri;
	//activemq://username:password@host:port
	protected ActivemqConnection(URISpec uri, String... supportedSchema) throws IOException {
		super(uri, supportedSchema);
		this.uri = uri;
	}

	public ActivemqConnection(URISpec uri) throws IOException {
		this(uri, "activemq", "tcp");
	}

	@Override
    protected Connection initialize(URISpec uri) {
		
		String username = uri.getUsername();
		String password = uri.getPassword();
		String url = "tcp://" + uri.getHost();
		ActiveMQConnectionFactory connectionFactory = (username == null && password == null) ? new ActiveMQConnectionFactory(url) : new ActiveMQConnectionFactory(username, password, url);
		ActivemqConfig.config(connectionFactory, uri);
        Connection conn;
        try {
        	conn = connectionFactory.createConnection();
			conn.start();
			return conn;
		} catch (JMSException e) {
			throw new RuntimeException("Create Activemq connection failed.", e);
		}
    }
	
	public static class Driver implements net.butfly.albatis.Connection.Driver<ActivemqConnection> {
        static {
            DriverManager.register(new Driver());
        }

        @Override
        public ActivemqConnection connect(URISpec uriSpec) throws IOException {
            return new ActivemqConnection(uriSpec);
        }

        @Override
        public List<String> schemas() {
            return Colls.list("activemq", "tcp");
        }
    }

	@SuppressWarnings("unchecked")
	@Override
	public ActivemqInput inputRaw(TableDesc... table) throws IOException {
		return new ActivemqInput("ActivemqInput", this, table[0].qualifier.name);
	}

	@SuppressWarnings("unchecked")
	@Override
	public ActivemqOutput outputRaw(TableDesc... table) throws IOException {
		return new ActivemqOutput("ActivemqOutput", this, table[0].qualifier.name);
	}

	@Override
    public void close() {
        try {
            super.close();
        } catch (IOException e) {
            logger.error("Close failure", e);
        }
        try {
        	client.stop();
			client.close();
		} catch (JMSException e) {
			logger.error("Close failure", e);
		}
    }

	public String mode() {
		return uri.getParameter("mode");
	}
}
