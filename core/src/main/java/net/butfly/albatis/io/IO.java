package net.butfly.albatis.io;

import java.io.IOException;
import java.io.Serializable;
import java.util.Base64;

import com.hzcominfo.albatis.nosql.Connection;

import net.butfly.albacore.base.Sizable;
import net.butfly.albacore.io.Openable;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.serder.JavaSerder;
import net.butfly.albacore.utils.collection.Maps;

public interface IO extends IOSchemaness, Sizable, Openable, Serializable, IOStats {
	default Connection connect() throws IOException {
		return Connection.DUMMY;
	}

	default URISpec target() {
		return null;
	}

	@Override
	default void open() {
		stating();
		Openable.super.open();
	}

	default int batchSize() {
		IO b = Wrapper.bases(this);
		return Props.PROPS.computeIfAbsent(this, io -> Maps.of()).computeIfAbsent(Props.BATCH_SIZE, //
				k -> Props.propI(b.getClass(), k, 500)).intValue();
	}

	default int features() {
		return 0;
	}

	interface Feature {
		static final int STREAMING = 0x1;
		static final int WRAPPED = 0x02;
		static final int ODD = 0x04;
		static final int SPARK = 0x08;
	}

	default boolean hasFeature(int... f) {
		int f0 = features();
		for (int ff : f)
			f0 &= ff;
		return 0 != f0;
	}

	default String ser() {
		byte[] b = JavaSerder.toBytes(this);
		return Base64.getEncoder().encodeToString(b);
	}

	static <T extends IO> T der(String ser) {
		byte[] b = Base64.getDecoder().decode(ser);
		return JavaSerder.fromBytes(b);
	}
}
