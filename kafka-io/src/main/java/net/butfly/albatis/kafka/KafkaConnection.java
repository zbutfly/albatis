package net.butfly.albatis.kafka;

import java.io.IOException;
import java.util.List;

import org.apache.kafka.common.security.JaasUtils;

import com.hzcominfo.albatis.nosql.Connection;
import com.hzcominfo.albatis.nosql.DataConnection;

import kafka.utils.ZkUtils;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.TypelessIO;
import net.butfly.alserder.SerDes;
import scala.collection.JavaConversions;

public class KafkaConnection extends DataConnection<Connection> implements TypelessIO {
	public KafkaConnection(URISpec uri) throws IOException {
		super(uri, "kafka");
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public List<SerDes> serdes() {
		String biz = Configs.gets("albatis.format.biz.default", "etl"); // should be config by
		List<SerDes> sds = TypelessIO.super.serdes();
		SerDes sd;
		if (null != biz && sds.size() == 1 && null != (sd = SerDes.sd(biz))) sds.add(sd);
		return sds;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public KafkaInput inputRaw(TableDesc... topic) throws IOException {
		Class nativeClass = nativeSD().toClass();
		try {
			if (String.class.isAssignableFrom(nativeClass)) return new KafkaInput<>("KafkaInput", uri, String.class, topic);
			else if (byte[].class.isAssignableFrom(nativeClass)) return new KafkaInput<>("KafkaInput", uri, byte[].class, topic);
			else throw new IllegalArgumentException();
		} catch (ConfigException e) {
			throw new IllegalArgumentException(e);
		}
	}

	@Override
	public KafkaOutput outputRaw(TableDesc... topic) throws IOException {
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

	// @Override
	// public void construct(String dbName, String table, TableDesc tableDesc, List<FieldDesc> fields) {
	// String kafkaUrl = uri.getHost()+"/kafka";
	// ZkUtils zkUtils = ZkUtils.apply(kafkaUrl, 30000, 30000, JaasUtils.isZkSecurityEnabled());
	// Integer partition = (Integer) tableDesc.construct.get("partition");
	// Integer replication = (Integer) tableDesc.construct.get("replication");
	// AdminUtils.createTopic(zkUtils, table, partition, replication, new Properties(), new RackAwareMode.Enforced$());
	// logger().info("create kafka topic successful");
	// zkUtils.close();
	// }

	@Override
	public boolean judge(String dbName, String table) {
		String kafkaUrl = uri.getHost() + "/kafka";
		ZkUtils zkUtils = ZkUtils.apply(kafkaUrl, 30000, 30000, JaasUtils.isZkSecurityEnabled());
		List<String> topics = JavaConversions.seqAsJavaList(zkUtils.getAllTopics());
		return topics.contains(table);
	}
}
