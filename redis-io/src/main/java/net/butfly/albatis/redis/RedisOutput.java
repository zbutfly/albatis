package net.butfly.albatis.redis;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.OutputBase;
import net.butfly.albatis.io.Rmap;

public class RedisOutput<T> extends OutputBase<Rmap> {
	private static final long serialVersionUID = -4110089452435157612L;
	private final static String KEY_PREFIX = Configs.get("albatis.redis.key.prefix", "DPC:CODEMAP:");
	private final static String KEY_EXPIRE_FIELD = Configs.get("albatis.redis.key.expire.field");

	private final RedisConnection<T> conn;
	private final Map<String, TableDesc> descs = Maps.of();
	private final boolean key_expire;

	public RedisOutput(RedisConnection<T> conn, TableDesc... prefix) {
		super("RedisOutput");
		this.conn = conn;
		key_expire = KEY_EXPIRE_FIELD != null;
		if (!Colls.empty(prefix)) for (TableDesc t : prefix)
			descs.put(t.qualifier.name, t);
	}

	@Override
	public void close() {
		super.close();
		conn.close();
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void enqsafe(Sdream<Rmap> msgs) {
		List<Rmap> retry = Colls.list();
		AtomicInteger c = new AtomicInteger();
		msgs.eachs(m -> {
			if (Colls.empty(m)) return;
			List<Pair<String, T>> fails = Colls.list();
			m.forEach((fieldname, fieldvalue) -> {
				Pair<String, T> r = send(key(m.table().name, fieldname), fieldname, (T) fieldvalue, m);
				if (null == r) c.incrementAndGet();
				else fails.add(r);
			});
			Rmap failed = m.skeleton();
			fails.forEach(p -> m.put(p.v1(), p.v2()));
			retry.add(failed);
		});
		failed(Sdream.of(retry));
		succeeded(c.get());
	}

	private T key(String table, String key) {
		if (Colls.empty(table) || Colls.empty(key)) return null;
		String kk = KEY_PREFIX + table.replaceAll(":", "_") + ":" + key;
		return conn.keying(kk);
	}

	// return non-null for retry
	private Pair<String, T> send(T rk, String fieldname, T fieldvalue, Rmap m) {
		if (null == rk) return null;
		logger().trace("Redis send: " + rk + " => " + fieldvalue);
		String r;
		long expireSec = -1;
		if (key_expire && (expireSec = getExpireSecond(m.get(KEY_EXPIRE_FIELD))) > 0) r = conn.sync.setex(rk, expireSec, fieldvalue);
		else r = conn.sync.set(rk, fieldvalue);
		if (null != r && "OK".equals(r)) return null;
		else return new Pair<>(fieldname, fieldvalue);
	}

	private static long getExpireSecond(Object keyExpireField) {
		if (Number.class.isAssignableFrom(keyExpireField.getClass())) {
			return ((Number) keyExpireField).longValue();
		} else if (Date.class.isAssignableFrom(keyExpireField.getClass())) {
			return (((Date) keyExpireField).getTime() - new Date().getTime()) / 1000;
		} else return -1;
	}
}
