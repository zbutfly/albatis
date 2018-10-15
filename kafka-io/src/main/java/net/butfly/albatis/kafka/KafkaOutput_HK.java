package net.butfly.albatis.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.OutputBase;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.kafka.config.KafkaOutputConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * @author zqh
 */
public final class KafkaOutput_HK extends OutputBase<Rmap> {
	private final URISpec uri;
	private final KafkaOutputConfig config;
	private final Producer<byte[], byte[]> producer;
	private final Function<Map<String, Object>, byte[]> coder;

	public KafkaOutput_HK(String name, URISpec kafkaURI, Function<Map<String, Object>, byte[]> coder) throws ConfigException {
		super(name);
		this.coder = coder;
		uri = kafkaURI;
		config = new KafkaOutputConfig(uri);
		producer = new Producer<byte[], byte[]>(config.getConfig());
		closing(producer::close);
	}

	@Override
	protected void enqsafe(Sdream<Rmap> messages) {
		List<Rmap> msgs = messages.list();
		List<Rmap> maps = new ArrayList<>();
		ObjectMapper mapper = new ObjectMapper();
		for(int i=0;i<msgs.size();i++){
			Map m = Maps.of();
			String key =msgs.get(i).key().toString();
			String table = msgs.get(i).table();
			m.put("rowkey",msgs.get(i).get(key));
			m.put("Body",msgs.get(i));
			String mapJsonStr ="";
			try {
				mapJsonStr = mapper.writeValueAsString(m);
			} catch (JsonProcessingException e) {
				e.printStackTrace();
			}
			Rmap message = new Rmap();
			message.put(msgs.get(i).get(key).toString(),mapJsonStr);
			message.key(key);
			message.table(table);
			maps.add(message);
		}
		List<KeyedMessage<byte[], byte[]>> ms = of(maps).map(m -> Kafkas.toKeyedMessage(m, coder)).nonNull().list();
		if (!ms.isEmpty()) try {
			int s = ms.size();
			if (s == 1) producer.send(ms.get(0));
			else producer.send(ms);
			succeeded(s);
		} catch (Exception e) {
			failed(Sdream.of(msgs));
		}
	}

	public String getDefaultTopic() {
		return config.topics().isEmpty() ? null : config.topics().get(0);
	}
}
