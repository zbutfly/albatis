package com.hikvision.multidimensional.kafka.register.schema;

import java.util.Arrays;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaConsumerDemo {
	public static void main(String[] args) throws Exception {
		Properties props = new Properties();
		props.put("schema.registry.url", KafkaDemo.SCHEMA_REG);
		props.put("bootstrap.servers", KafkaDemo.BOOTSTRAP);
		props.put("group.id", KafkaDemo.GROUP_ID);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
		ConsumerRecords<String, GenericRecord> records;
		try (KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props);) {
			consumer.subscribe(Arrays.asList(KafkaDemo.TOPIC));
			while (true) {
				records = consumer.poll(10);
				if (records.isEmpty()) System.err.println("No records fetched");
				else records.forEach(record -> {
					GenericRecord user = record.value();
					System.out.println("value = [user.id = " + user.get("id") + ", " + "user.name = " + user.get("name") + ", "
							+ "user.age = " + user.get("age") + "], " + "partition = " + record.partition() + ", " + "offset = " + record
									.offset());
				});
			}
		}
	}
}
