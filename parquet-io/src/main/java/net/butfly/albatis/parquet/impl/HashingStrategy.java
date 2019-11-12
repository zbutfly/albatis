package net.butfly.albatis.parquet.impl;

public interface HashingStrategy {
	String hashing(Object value);

	static HashingStrategy get(String impl, String... args) {
		if (null == impl) return null;
		String[] s = impl.split(":", 2);
		switch (s[0]) {
		case "DATE":
			return new HashingByDateStrategy(s[1], args.length > 0 ? args[0] : null);
		case "HOUR":
			return new HashingByDateStrategy(s[1], args.length > 0 ? args[0] : null);
		default:
			throw new IllegalArgumentException("Strategy not define for: " + s[0]);
		}
	}
}
