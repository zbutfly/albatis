package net.butfly.albatis.kafka;

import java.io.IOException;
import java.util.List;

import com.hzcominfo.albatis.nosql.Connection;
import com.hzcominfo.albatis.nosql.NoSqlConnection;

import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.ddl.TableDesc;

public class KafkaConnection extends NoSqlConnection<Connection> {
	public KafkaConnection(URISpec uri) throws IOException {
		super(uri, "kafka");
	}

	@Override
	public KafkaInput input(TableDesc... topic) throws IOException {
		List<String> l = Colls.list(t -> t.name, topic);
		try {
			return new KafkaInput("KafkaInput", uri, l.toArray(new String[l.size()]));
		} catch (ConfigException e) {
			throw new IOException(e);
		}
	}

	@Override
	public KafkaOutput output(TableDesc... table) throws IOException {
		try {
			return new KafkaOutput("KafkaInput", uri);
		} catch (ConfigException e) {
			throw new IOException(e);
		}
	}

	public static class Driver implements com.hzcominfo.albatis.nosql.Connection.Driver<KafkaConnection> {
		static {
			DriverManager.register(new Driver());
		}

		@Override
		public KafkaConnection connect(URISpec uriSpec) throws IOException {
			return new KafkaConnection(uriSpec);
		}

		@Override
		public List<String> schemas() {
			return Colls.list("kafka");
		}
	}

	@Override
	protected Connection initialize(URISpec uri) {
		return null;
	}
}
