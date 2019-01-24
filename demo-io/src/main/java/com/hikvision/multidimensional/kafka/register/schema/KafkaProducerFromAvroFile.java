package com.hikvision.multidimensional.kafka.register.schema;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import net.butfly.albacore.utils.collection.Maps;

public class KafkaProducerFromAvroFile {
	private static AtomicLong count = new AtomicLong(), spent = new AtomicLong();

	public static void main(String[] args) throws IOException {
		Properties props = new Properties();
		props.put("schema.registry.url", KafkaDemo.SCHEMA_REG);
		props.put("bootstrap.servers", KafkaDemo.BOOTSTRAP);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");

		File f = new File("part-00000-d057c248-84d8-4057-9c1a-b93c1dff2e68-c000.avro");
		DatumReader<GenericRecord> r = new GenericDatumReader<>();
		Schema schema;
		try (InputStream ss = KafkaProducerFromAvroFile.class.getResourceAsStream("/dpc-schema.json")) {
			schema = new Schema.Parser().parse(ss);
		}
		try (DataFileReader<GenericRecord> fr = new DataFileReader<>(f, r);
				Producer<String, GenericRecord> producer = new KafkaProducer<>(props);) {
			while (fr.hasNext()) {
				GenericRecord rec = fr.next();

				Map<String, Object> m = Maps.of();
				Object v;
				for (Schema.Field ff : rec.getSchema().getFields())
					if (null != (v = rec.get(ff.name()))) m.put(ff.name(), v);

				GenericRecord rec2 = new GenericData.Record(schema);
				schema.getFields().forEach(fff -> rec2.put(fff.name(), m.get(fff.name())));

				long now = System.currentTimeMillis();
				try {
					producer.send(new ProducerRecord<>(KafkaDemo.TOPIC, rec2));
				} finally {
					long t = spent.addAndGet(System.currentTimeMillis() - now) / 1000;
					long c = count.incrementAndGet();
					if (t > 1 && c % 1000 == 0) //
						System.err.println("Processed [" + c + " recs], spent [" + t + " s] and [" + c / t + " recs/sec].");
				}
			}
		}
	}
}
