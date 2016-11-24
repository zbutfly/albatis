package net.butfly.albatis.dao;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import net.butfly.albacore.exception.ConfigException;
import net.butfly.albatis.kafka.KafkaInput;
import net.butfly.albatis.kafka.KafkaMessage;

public class KafkaTest {
	public static void main(String[] args) throws ConfigException, IOException {
		Map<String, Integer> topics = new HashMap<>();
		topics.put("HZGA_GAZHK_CZRK", 3);
		topics.put("HZGA_GAZHK_LGY_NB", 3);
		try (KafkaInput in = new KafkaInput("KafkaInput", "kafka.properties", topics);) {
			do {
				for (KafkaMessage m : in.dequeue(100))
					System.err.println("Fetched: " + m.toString());
			} while (true);
		}
	}
}
