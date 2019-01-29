package net.butfly.albatis.kafka.avro;

import static net.butfly.albacore.utils.collection.Colls.empty;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.Systems;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.alserdes.avro.AvroSerDes.Builder;

public class SchemaReg {
	private static final Logger logger = Logger.getLogger(SchemaReg.class);
	private static final String BASIC_CONF = "schema.registry.url";

	public static final String SCHEMA_REGISTRY_URL_CONF = "albatis.kafka.schema.registry.url";
	public static final String SCHEMA_REGISTRY_PROPS_FILE_CONF = "albatis.kafka.schema.registry.config";
	static {
		logger.info(SchemaReg.class.getName() + " loaded, settings: \n\t"//
				+ "[" + SCHEMA_REGISTRY_PROPS_FILE_CONF + "=properties filename] priority and [" + BASIC_CONF + "] required, or \n\t"//
				+ "[" + SCHEMA_REGISTRY_URL_CONF + "=schema registry url].");
	}

	private final KafkaAvroSerializer ser;
	private final KafkaAvroDeserializer deser;

	SchemaReg() {
		super();
		Map<String, Object> configs = load();
		ser = registrySerializer(configs);
		deser = registryDeserializer(configs);
	}

	private static AtomicLong count = new AtomicLong(), spent = new AtomicLong(), size = new AtomicLong();

	public byte[] ser(String topic, Map<String, Object> m, Schema schema) {
		if (null == ser) return Builder.ser(m, schema);
		long now = System.currentTimeMillis();
		byte[] b = null;
		try {
			b = ser.serialize(topic, Builder.rec(m, schema));
		} finally {
			if (Systems.isDebug()) {
				long t = spent.addAndGet(System.currentTimeMillis() - now) / 1000;
				long c = count.incrementAndGet();
				long kb = size.addAndGet(b.length) / 1000;
				if (t > 1 && c % 50000 == 0) //
					logger.trace("\n\tSchema registry sent [" + c + " recs], spent [" + t + " s], size [" + kb + " kb], \n"//
							+ "\t\tavg [" + kb * 1000 / c + " bytes/rec] and [" + c / t + " recs/sec].");
			}
		}
		return b;
	}

	public Map<String, Object> deser(String topic, byte[] v, Schema schema) {
		if (null == deser) return Builder.deser(v, schema);
		GenericRecord rec = (GenericRecord) deser.deserialize(topic, v);
		if (null == rec.getSchema() || empty(rec.getSchema().getFields())) return null;
		Map<String, Object> m = Maps.of();
		rec.getSchema().getFields().forEach(field -> m.put(field.name(), rec.get(field.name())));
		return m;
	}

	private static Map<String, Object> load() {
		String fname = Configs.gets(SCHEMA_REGISTRY_PROPS_FILE_CONF);
		String regurl;
		final Map<String, Object> m = Maps.of();
		if (null == fname) {
			if (null != (regurl = Configs.gets(SCHEMA_REGISTRY_URL_CONF))) m.put(BASIC_CONF, regurl);
		} else {
			Properties props = new Properties();
			try {
				props.load(SchemaReg.class.getResourceAsStream(fname));
			} catch (IOException e) {
				logger.error("Schema registry disabled for configuration file [" + fname + "] loading fail.", e);
				return null;
			}
			Maps.of(props).forEach((k, v) -> m.put(k, v));
			regurl = (String) m.get(BASIC_CONF);
			if (empty(regurl)) m.clear();
		}
		if (empty(m)) {
			logger.debug("Schema registry disabled for basic config [" + BASIC_CONF + "] not defined.");
			return null;
		} else {
			logger.info("Schema registry enabled as coniguration:\n\t" + m.toString());
			return m;
		}
	}

	private static KafkaAvroSerializer registrySerializer(Map<String, Object> configs) {
		if (empty(configs)) return null;
		KafkaAvroSerializer s = new KafkaAvroSerializer();
		s.configure(configs, false);
		return s;
	}

	private static KafkaAvroDeserializer registryDeserializer(Map<String, Object> configs) {
		if (empty(configs)) return null;
		KafkaAvroDeserializer d = new KafkaAvroDeserializer();
		d.configure(configs, false);
		return d;
	}
}