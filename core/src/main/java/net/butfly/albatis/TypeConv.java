package net.butfly.albatis;

import java.io.Serializable;
import java.util.Map;

import net.butfly.albacore.io.lambda.BiFunction;
import net.butfly.albatis.ddl.vals.ValType;

public interface TypeConv<T, R> extends Serializable {
	T conv(ValType t);// ser from java into db

	ValType unconv(T t); // deser from db into java

	R write(Map<String, Object> vals, BiFunction<String, Object, ValType> typing);

	Map<String, Object> read(R rec, BiFunction<String, R, T> typing);
}
