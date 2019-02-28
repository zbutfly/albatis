package net.butfly.albatis.kafka;

import static net.butfly.alserdes.format.Format.of;

import java.io.IOException;
import java.util.List;

import org.apache.kafka.common.security.JaasUtils;

import kafka.utils.ZkUtils;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.Connection;
import net.butfly.albatis.DataConnection;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.IOFactory;
import net.butfly.alserdes.SerDes;
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
			return null == def ? fmts : Colls.list(fmts.get(0), def);
		} else return fmts;
	}

	@SuppressWarnings("unchecked")
	@Override
	public KafkaIn inputRaw(TableDesc... topic) throws IOException {
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

	@Override
	public boolean judge(String table) {
		String[] tables = table.split("\\.");
		String dbName, tableName;
		if (tables.length == 1)
			dbName = tableName = tables[0];
		else if ((tables.length == 2)) {
			dbName = tables[0];
			tableName = tables[1];
		} else throw new RuntimeException("Please type in corrent es table format: db.table !");
		String kafkaUrl = uri.getHost() + "/kafka";
		ZkUtils zkUtils = ZkUtils.apply(kafkaUrl, 30000, 30000, JaasUtils.isZkSecurityEnabled());
		List<String> topics = JavaConversions.seqAsJavaList(zkUtils.getAllTopics());
		return topics.contains(tableName);
	}
}
