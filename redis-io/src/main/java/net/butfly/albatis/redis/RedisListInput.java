package net.butfly.albatis.redis;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.OddInput;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.redis.key.RedisKey;

public class RedisListInput<T> extends net.butfly.albacore.base.Namedly implements OddInput<Rmap> {
	private static final long serialVersionUID = -1411141076610748159L;

	private final RedisConnection<T> conn;
	private final BlockingQueue<RedisKey> keys = new LinkedBlockingQueue<>();
	private final Map<T, TableDesc> descs = Maps.of();

	public RedisListInput(RedisConnection<T> conn, TableDesc... table) {
		super("RedisListInput");
		this.conn = conn;
		if (!Colls.empty(table)) for (TableDesc t : table) {
			T k = conn.keying(t.qualifier.name);
			keys.add(RedisKey.setKey(k));
			descs.put(k, t);
		}
	}

	@Override
	public void close() {
		OddInput.super.close();
		conn.close();
	}

	@SuppressWarnings("unchecked")
	@Override
	public Rmap dequeue() {
		while (opened() && !keys.isEmpty()) {
			RedisKey rk = null;
			try {
				rk = keys.poll();
				if (null == (rk)) return null;
				T k = (T) rk.getKey();
				T v = conn.sync.lpop(k);
				if (null == v) return null;
				return new Rmap(descs.get(k).qualifier.name, Maps.of(UUID.randomUUID().toString(), v));
			} finally {
				if (null != rk)
					while (!keys.add(rk));
			}
		}
		return null;
	}

}
