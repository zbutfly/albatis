package net.butfly.albatis.redis;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.Utf8StringCodec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.OutputBase;
import net.butfly.albatis.io.Rmap;

public class RedisOutput extends OutputBase<Rmap> {
	private static final long serialVersionUID = -4110089452435157612L;
	private final StatefulRedisConnection<String, String> src;
	private final RedisCommands<String, String> syncCommands;
	private final String prefix = Configs.get("albatis.redis.key.prefix", "DPC:CODEMAP:");

	public RedisOutput(String name, RedisConnection redisConn) {
		this(name, redisConn, new Utf8StringCodec());
	}

	public RedisOutput(String name, RedisConnection redisConn, RedisCodec<String, String> redisCodec) {
		super(name);
		src = redisConn.redisClient.connect(redisCodec);
		syncCommands = src.sync();
		closing(this::close);
	}

	@Override
	protected void enqsafe(Sdream<Rmap> msgs) {
		List<Rmap> fails = Colls.list();
		AtomicInteger c = new AtomicInteger();
		msgs.eachs(m -> {
			if (!Colls.empty(m)) m.forEach((k, v) -> send(k, v, m.table(), m.key(), fails, c));
		});
		this.failed(Sdream.of(fails));
		succeeded(c.get());
	}

	private void send(String k, Object v, String table, Object key, List<Rmap> fails, AtomicInteger c) {
		String kk = prefix + table.replaceAll(":", "") + ":" + key;
		String r = syncCommands.set(kk, v.toString());
		if (null != r && "OK".equals(r)) c.getAndIncrement();
		else fails.add(new Rmap(table, key, Maps.of(k, v)));
	}

	@Override
	public void close() {
		src.close();
	}
}
