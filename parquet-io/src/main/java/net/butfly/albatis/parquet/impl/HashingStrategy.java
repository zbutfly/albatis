package net.butfly.albatis.parquet.impl;

public interface HashingStrategy {
	String hashing(Object value);

	static HashingStrategy get(String impl) {
		if (null == impl) return null;
		String[] s = impl.split(":", 2);
		switch (s[0]) {
		case "DATE":
			return new HashingByDateStrategy(s[1]);
		case "HOUR":
			return new HashingByDateStrategy(s[1]);
		default:
			throw new IllegalArgumentException("Strategy not define for: " + s[0]);
		}
	}
}
