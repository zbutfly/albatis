package net.butfly.albatis.kafka;

import java.io.IOException;
import java.util.List;

import com.hzcominfo.albatis.nosql.NoSqlConnection;

import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.serder.BsonSerder;
import net.butfly.albacore.utils.collection.Colls;

public class KafkaConnection extends NoSqlConnection<Object> {
	public KafkaConnection(URISpec uri) throws IOException {
		super(uri, u -> null, "kafka");
	}

	@Override
	public KafkaInput input(String... topic) throws IOException {
		try {
			return new KafkaInput("KafkaInput", uri, BsonSerder::map, topic);
		} catch (ConfigException e) {
			throw new IOException(e);
		}
	}

	@Override
	public KafkaOutput output() throws IOException {
		try {
			return new KafkaOutput("KafkaInput", uri, BsonSerder::map);
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
			return Colls.list("mongodb");
		}
	}
}
