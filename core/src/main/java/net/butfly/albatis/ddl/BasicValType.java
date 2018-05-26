package net.butfly.albatis.ddl;

public final class BasicValType extends ValType {
	public final long length;

	private BasicValType(Class<?> rawClass, Class<?> boxedClass, int jdbcType, long length, String... flags) {
		super(rawClass, boxedClass, jdbcType, flags);
		this.length = length;
	}

	BasicValType(Class<?> rawClass, Class<?> boxedClass, int jdbcType, String... flags) {
		this(rawClass, boxedClass, jdbcType, -1, flags);
	}

	@Override
	public BasicValType len(long len) {
		return new BasicValType(rawClass, boxedClass, jdbcType, len, flag);
	}
}
