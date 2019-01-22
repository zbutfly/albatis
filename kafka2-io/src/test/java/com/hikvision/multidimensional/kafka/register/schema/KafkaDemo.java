package com.hikvision.multidimensional.kafka.register.schema;

import org.apache.avro.Schema;

public interface KafkaDemo {
	static final String BOOTSTRAP = "10.41.2.68:9092";// "10.3.68.131:9092"
	static final String SCHEMA_REG = "http://10.41.2.68:8081";// "http://10.3.68.131:8081"
	static final String TOPIC = "benchmark_test";// "origin_test_zx";
	static final String GROUP_ID = "origin_test_001";
	static final String USER_SCHEMA_JSON = "{\"type\": \"record\", \"name\": \"User\", \"fields\": ["//
			+ "{\"name\": \"id\", \"type\": \"int\"}, "//
			+ "{\"name\": \"name\",  \"type\": \"string\"}, "//
			+ "{\"name\": \"age\", \"type\": \"int\"}"//
			+ "]}";
	static final Schema USER_SCHEMA = new Schema.Parser().parse(USER_SCHEMA_JSON);
}
