package net.butfly.albatis.redis;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import net.butfly.albacore.serder.JsonSerder;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.io.OddInput;
import net.butfly.albatis.io.Rmap;

public class RedisListInput extends net.butfly.albacore.base.Namedly implements OddInput<Rmap> {
	private static final long serialVersionUID = -1411141076610748159L;
	
	private static final Logger logger = Logger.getLogger(RedisListInput.class);
	private final RedisConnection redisConn;
	private final Object[] tableArray;
	private final BlockingQueue<Object> keys = new LinkedBlockingQueue<>();
	private final Map<Object, Boolean> isEmptyMap = new ConcurrentHashMap<>();

	public RedisListInput(String name, RedisConnection redisConn, Object... tables) {
		super(name);
		this.redisConn = redisConn;
		this.tableArray = tables;
	}
	
	@Override
	public void open() {
		Arrays.asList(tableArray).stream().forEach(t -> {
			while (!keys.offer(t));
			isEmptyMap.put(t, false);
		});
	}

	@Override
	public Rmap dequeue() {
		Object key;
		while (opened() && !keys.isEmpty()) {
			if (null != (key = keys.poll())) {
				if ("byteArray".equals(redisConn.type)) {
					byte[] bArr = redisConn.client.lpop((byte[]) key);
					return new Rmap(new String((byte[]) key), JsonSerder.JSON_MAPPER.fromBytes(bArr, HashMap.class));
				} else if ("string".equals(redisConn.type)) {
					String eStr = redisConn.client.lpop((String) key);
					return new Rmap((String) key, JsonSerder.JSON_MAPPER.der(eStr, HashMap.class));
				} else {
					
				}
			}
		}
		
		return null;
	}

}
