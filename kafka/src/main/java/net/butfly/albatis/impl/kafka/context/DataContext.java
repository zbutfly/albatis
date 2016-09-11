package net.butfly.albatis.impl.kafka.context;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.butfly.albatis.impl.kafka.mapper.KafkaMessage;

public class DataContext {
	// kafka consumer的基础输出
	public static List<KafkaMessage> msgList = Collections.synchronizedList(new ArrayList<>());

	public static Map<String, List<KafkaMessage>> msgMap = Collections.synchronizedMap(new HashMap<>());

	public static List<List<KafkaMessage>> msgPcgList = Collections.synchronizedList(new ArrayList<>());

}
