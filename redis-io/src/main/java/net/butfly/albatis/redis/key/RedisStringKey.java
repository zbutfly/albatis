package net.butfly.albatis.redis.key;

public class RedisStringKey implements RedisKey {

	private String key;

	protected RedisStringKey(String key) {
		this.key = key;
	}

	@Override
	public Object getKey() {
		return key;
	}
}
