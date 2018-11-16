package net.butfly.albatis.redis.key;

public interface RedisKey {

	public Object getKey();
	public static RedisKey setKey(Object key) {
		if (key instanceof String) {
			return new RedisStringKey((String) key);
		} else if (key instanceof byte[]) {
			return new RedisByteArrayKey((byte[]) key);
		}
		return null;
	}
}
