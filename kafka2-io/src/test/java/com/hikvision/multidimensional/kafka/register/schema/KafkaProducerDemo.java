package com.hikvision.multidimensional.kafka.register.schema;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class KafkaProducerDemo {

	public static void main(String[] args) throws Exception {
		Properties props = new Properties();
		props.put("schema.registry.url", KafkaDemo.SCHEMA_REG);
		props.put("bootstrap.servers", KafkaDemo.BOOTSTRAP);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
		Random rand = new Random();
		int id = 0;
		try (Producer<String, GenericRecord> producer = new KafkaProducer<>(props);) {
			while (id < 10)
				send(++id, KafkaDemo.USER_SCHEMA, rand, producer);
		}
	}

	private static void send(int id, Schema schema, Random rand, Producer<String, GenericRecord> producer) {
		String name = "name" + id;
		int age = rand.nextInt(40) + 1;
		GenericRecord user = new GenericData.Record(schema);
		user.put("id", id);
		user.put("name", name);
		user.put("age", age);
		ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(KafkaDemo.TOPIC, user);
		RecordMetadata r;
		try {
			r = producer.send(record).get();
			System.out.println(name + ": sent, r: " + r.topic() + "-" + r.partition() + "#" + r.offset());
		} catch (InterruptedException e) {} catch (ExecutionException e) {
			throw new RuntimeException(name + ": sent, err: ", e.getCause());
		}
	}
}
