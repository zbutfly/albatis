package net.butfly.albatis.kafka.old;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import net.butfly.albacore.exception.ConfigException;

public class KafkaProducerTest {
	public static void main(String[] args) throws ConfigException, IOException {
		Map<String, Object> props = new HashMap<>();
		props.put("bootstrap.servers", "hzga136:9092,hzga137:9092,hzga138:9092,hzga139:9092,hzga140:9092,hzga141:9092");
		props.put("key.serializer", ByteArraySerializer.class.getName());
		props.put("value.serializer", ByteArraySerializer.class.getName());
		props.put("max.block.ms", 1000);
		try (KafkaProducer<byte[], byte[]> p = new KafkaProducer<>(props);) {
			for (PartitionInfo part : p.partitionsFor("HZGA_GAZHK_CZRK"))
				System.err.println(part.toString());
		}
		System.out.println("finished.");
	}
}
