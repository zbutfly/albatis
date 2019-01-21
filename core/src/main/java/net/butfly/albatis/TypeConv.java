package net.butfly.albatis;

import java.io.Serializable;
import java.util.function.Function;

import net.butfly.albatis.ddl.vals.ValType;

public interface TypeConv<T> extends Serializable {
	ValType conv(T t);

	T unconv(ValType t);

	static <T> TypeConv<T> of(Function<T, ValType> conv, Function<ValType, T> unconv) {
		return new TypeConv<T>() {
			private static final long serialVersionUID = 244789724685870487L;

			@Override
			public ValType conv(T t) {
				return conv.apply(t);
			}

			@Override
			public T unconv(ValType t) {
				return unconv(t);
			}
		};
	}
}
