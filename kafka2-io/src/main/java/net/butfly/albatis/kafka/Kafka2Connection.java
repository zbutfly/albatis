package net.butfly.albatis.kafka;

import static net.butfly.alserdes.format.Format.of;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.common.security.JaasUtils;

import kafka.utils.ZkUtils;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.Connection;
import net.butfly.albatis.DataConnection;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.IOFactory;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Rmap;
import net.butfly.alserdes.SerDes;
import net.butfly.alserdes.format.ConstFormat;
import net.butfly.alserdes.format.Format;
import scala.collection.JavaConversions;

@SerDes.As("bson")
public class Kafka2Connection extends DataConnection<Connection> implements IOFactory {
	protected Kafka2Connection(URISpec uri, String... supportedSchema) throws IOException {
		super(uri, supportedSchema);
	}

	public Kafka2Connection(URISpec uri) throws IOException {
		this(uri, "kafka2");
	}

	@SuppressWarnings("rawtypes")
	@Override
	public List<Format> formats() {
		List<Format> fmts = super.formats();
		if (null != fmts && fmts.size() == 1 && fmts.get(0).equals(of("bson"))) {
			Format def = of(Configs.gets("albatis.format.biz.default", "etl"));
			return null == def || def instanceof ConstFormat ? fmts : Colls.list(fmts.get(0), def);
		} else return fmts;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Input<Rmap> inputRaw(TableDesc... topic) throws IOException {
		try {
			return new Kafka2Input("Kafka2Input", uri, topic);
		} catch (ConfigException e) {
			throw new IllegalArgumentException(e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public KafkaOut outputRaw(TableDesc... topic) throws IOException {
		try {
			return new Kafka2Output("Kafka2Output", uri);
		} catch (ConfigException e) {
			throw new IOException(e);
		}
	}

	public static class Driver implements net.butfly.albatis.Connection.Driver<Kafka2Connection> {
		static {
			DriverManager.register(new Driver());
		}

		@Override
		public Kafka2Connection connect(URISpec uriSpec) throws IOException {
			return new Kafka2Connection(uriSpec);
		}

		@Override
		public List<String> schemas() {
			return Colls.list("kafka2");
		}
	}

	@Override
	protected Connection initialize(URISpec uri) {
		return null;
	}

	private static Object RACK_AWARE_MODE;
	private static Method CREATE_TOPIC;
	static {
		try {
			RACK_AWARE_MODE = Class.forName("kafka.admin.RackAwareMode.Enforced$").newInstance();
			CREATE_TOPIC = Class.forName("kafka.admin.AdminUtils").getMethod("createTopic", ZkUtils.class, String.class, int.class,
					int.class, Properties.class, RACK_AWARE_MODE.getClass());
		} catch (ClassNotFoundException | NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException e) {
			RACK_AWARE_MODE = null;
			CREATE_TOPIC = null;
		}
	}

	@Override
	public void construct(String table, TableDesc tableDesc, List<FieldDesc> fields) {
		String tableName;
		String kafkaUrl;
		String[] tables = table.split("\\.");
		if (tables.length == 1) tableName = tables[0];
		else if (tables.length == 2) tableName = tables[1];
		else throw new RuntimeException("Please type in correct kafka table format: db.table !");
		if (!uri.toString().contains("?")) kafkaUrl = uri.toString().substring(uri.toString().indexOf("//") + 2);
		else kafkaUrl = uri.toString().substring(uri.toString().indexOf("//") + 2, uri.toString().indexOf("?"));
		ZkUtils zkUtils = ZkUtils.apply(kafkaUrl, 30000, 30000, JaasUtils.isZkSecurityEnabled());
		int partition = Integer.parseInt(tableDesc.construct.get("number_of_shards").toString());
		int replication = Integer.parseInt(tableDesc.construct.get("number_of_replicas").toString());

		try {
			CREATE_TOPIC.invoke(null, zkUtils, tableName, partition, replication, new Properties(), RACK_AWARE_MODE);
		} catch (IllegalAccessException | InvocationTargetException e) {
			throw new IllegalArgumentException(e);
		}
		logger().info("create kafka topic successful");
		zkUtils.close();
	}

	@Override
	public boolean judge(String table) {
		String tableName;
		String[] tables = table.split("\\.");
		if (tables.length == 1) tableName = tables[0];
		else if (tables.length == 2) tableName = tables[1];
		else throw new RuntimeException("Please type in correct kafka table format: db.table !");
		String kafkaUrl = uri.getHost() + "/kafka";
		ZkUtils zkUtils = ZkUtils.apply(kafkaUrl, 30000, 30000, JaasUtils.isZkSecurityEnabled());
		List<String> topics = JavaConversions.seqAsJavaList(zkUtils.getAllTopics());
		return topics.contains(tableName);
	}
}
