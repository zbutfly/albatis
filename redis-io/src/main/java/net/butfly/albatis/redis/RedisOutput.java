package net.butfly.albatis.redis;

import java.util.concurrent.atomic.AtomicLong;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.Utf8StringCodec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.serder.JsonSerder;
import net.butfly.albacore.utils.Configs;
import net.butfly.albatis.io.OutputBase;
import net.butfly.albatis.io.Rmap;

public class RedisOutput extends OutputBase<Rmap> {

	private static final long serialVersionUID = -4110089452435157612L;
	private final RedisConnection redisConn;
	private final StatefulRedisConnection<?, ?> src;
	private final RedisCommands syncCommands;
	private final String prefix = Configs.get("albatis.redis.key.prefix", "DPC:CODEMAP:");

	public RedisOutput(String name, RedisConnection redisConn) {
		this(name, redisConn, new Utf8StringCodec());
	}

	public RedisOutput(String name, RedisConnection redisConn, RedisCodec<?, ?> redisCodec) {
		super(name);
		this.redisConn = redisConn;
		src = redisConn.redisClient.connect(redisCodec);
		syncCommands = src.sync();
		closing(this::close);
	}

	@Override
	protected void enqsafe(Sdream<Rmap> msgs) {
		AtomicLong n = new AtomicLong();
		n.set(msgs.map(m -> syncCommands.set(prefix + m.table() + ":" + m.key(), JsonSerder.JSON_MAPPER.ser(m))).filter(s -> "OK".equals(s)).count());
		succeeded(n.get());
	}

	@Override
	public void close() {
		src.close();
	}
}
