package net.butfly.albatis.parquet.impl;

public interface PartitionStrategy {
	String partition(Object value);

	static PartitionStrategy get(String impl, String... args) {
		if (null == impl) return null;
		String[] s = impl.split(":", 2);
		switch (s[0]) {
		case "DATE":
			return new PartitionByDateStrategy(s[1], args.length > 0 ? args[0] : null);
		case "HOUR":
			return new PartitionByDateStrategy(s[1], args.length > 0 ? args[0] : null);
		default:
			throw new IllegalArgumentException("Strategy not define for: " + s[0]);
		}
	}
}
