package net.butfly.albatis.ddl.vals;

public final class BasicValType extends ValType {
	private static final long serialVersionUID = -9209846793567583303L;
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
