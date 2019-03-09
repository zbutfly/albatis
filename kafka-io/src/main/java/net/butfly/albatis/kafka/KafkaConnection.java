package net.butfly.albatis.kafka;

import java.io.IOException;
import java.util.List;

import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableDesc;

public class KafkaConnection extends Kafka2Connection {
	public KafkaConnection(URISpec uri) throws IOException {
		super(uri, "kafka");
	}

	@Override
	public KafkaIn inputRaw(TableDesc... topic) throws IOException {
		try {
			return new KafkaInput("KafkaInput", uri, topic);
		} catch (ConfigException e) {
			throw new IllegalArgumentException(e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public KafkaOut outputRaw(TableDesc... topic) throws IOException {
		try {
			return new KafkaOutput("KafkaOutput", uri);
		} catch (ConfigException e) {
			throw new IOException(e);
		}
	}

	@Override
	public void construct(String table, TableDesc tableDesc, List<FieldDesc> fields) {
		throw new UnsupportedOperationException();
	}

	public static class Driver implements net.butfly.albatis.Connection.Driver<KafkaConnection> {
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
}
