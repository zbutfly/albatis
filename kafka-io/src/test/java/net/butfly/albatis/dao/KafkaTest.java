package net.butfly.albatis.dao;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import net.butfly.albacore.exception.ConfigException;
import net.butfly.albatis.kafka.KafkaInput;

public class KafkaTest {
	public static void main(String[] args) throws ConfigException, IOException {
		Map<String, Integer> topics = new HashMap<>();
		topics.put("HZGA_GAZHK_CZRK", 1);
		try (KafkaInput in = new KafkaInput("KafkaInput", "kafka.properties", topics);) {
			//			in.enqueue0(it);

		}

	}
}
