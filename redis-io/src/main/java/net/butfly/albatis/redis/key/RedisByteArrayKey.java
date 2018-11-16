package net.butfly.albatis.redis.key;

public class RedisByteArrayKey implements RedisKey {

	private byte[] key;

	protected RedisByteArrayKey(byte[] key) {
		this.key = key;
	}

	@Override
	public Object getKey() {
		return key;
	}
}
