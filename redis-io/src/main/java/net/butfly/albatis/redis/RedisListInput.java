package net.butfly.albatis.redis;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.RedisCodec;
import net.butfly.albacore.serder.JsonSerder;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.io.TypelessInput;
import net.butfly.albatis.io.OddInput;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.redis.key.RedisKey;

public class RedisListInput extends net.butfly.albacore.base.Namedly implements OddInput<Rmap>, TypelessInput {
	private static final long serialVersionUID = -1411141076610748159L;

	private static final Logger logger = Logger.getLogger(RedisListInput.class);
	private final RedisConnection redisConn;
	private final Object[] keyArr;
	private final BlockingQueue<RedisKey> keys = new LinkedBlockingQueue<>();
	private final Map<RedisKey, Boolean> isEmptyMap = new ConcurrentHashMap<>();
	private final StatefulRedisConnection<?, ?> src;
	private final RedisCommands syncCommands;

	public RedisListInput(String name, RedisConnection redisConn, RedisCodec<?, ?> redisCodec, Object... tables) {
		super(name);
		this.redisConn = redisConn;
		this.keyArr = tables;
		this.src = redisConn.redisClient.connect(redisCodec);
		syncCommands = src.sync();

	}

	@Override
	public void open() {
		OddInput.super.open();
		Arrays.asList(keyArr).forEach(t -> {
			RedisKey key = RedisKey.setKey(t);
			isEmptyMap.put(key, false);
			keys.add(key);
		});
		closing(this::closeRedis);
	}

	@Override
	public Rmap dequeue() {
		RedisKey key;
		while (opened() && !keys.isEmpty()) {
			if (null != (key = keys.poll())) {
				try {
					if ("byteArray".equals(redisConn.type)) {
						byte[] bArr = (byte[]) syncCommands.lpop((byte[]) key.getKey());
						if (bArr != null) return new Rmap(new String((byte[]) key.getKey()), JsonSerder.JSON_MAPPER.fromBytes(bArr,
								HashMap.class));
					} else if ("string".equals(redisConn.type)) {
						String eStr = (String) syncCommands.lpop((String) key.getKey());
						if (eStr != null) return new Rmap((String) key.getKey(), JsonSerder.JSON_MAPPER.der(eStr, HashMap.class));
					} else {
						throw new UnsupportedOperationException();
					}
				} finally {
					if (null != key) keys.offer(key);
				}
				return null;
			}
		}
		return null;
	}

	public void closeRedis() {
		try {
			redisConn.close();
		} catch (IOException e) {
			logger.error("", e);
		}
	}
}
