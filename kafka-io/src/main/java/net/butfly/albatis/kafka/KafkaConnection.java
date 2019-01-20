package net.butfly.albatis.kafka;

import java.io.IOException;
import java.util.List;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Colls;

public class KafkaConnection extends Kafka2Connection {
	public KafkaConnection(URISpec uri) throws IOException {
		super(uri, "kafka");
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
