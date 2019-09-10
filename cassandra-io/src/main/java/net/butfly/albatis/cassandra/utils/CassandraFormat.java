package net.butfly.albatis.cassandra.utils;

import static net.butfly.albatis.ddl.vals.ValType.Flags.BINARY;
import static net.butfly.albatis.ddl.vals.ValType.Flags.BOOL;
import static net.butfly.albatis.ddl.vals.ValType.Flags.CHAR;
import static net.butfly.albatis.ddl.vals.ValType.Flags.DATE;
import static net.butfly.albatis.ddl.vals.ValType.Flags.DOUBLE;
import static net.butfly.albatis.ddl.vals.ValType.Flags.FLOAT;
import static net.butfly.albatis.ddl.vals.ValType.Flags.GEO;
import static net.butfly.albatis.ddl.vals.ValType.Flags.INT;
import static net.butfly.albatis.ddl.vals.ValType.Flags.JSON_STR;
import static net.butfly.albatis.ddl.vals.ValType.Flags.LONG;
import static net.butfly.albatis.ddl.vals.ValType.Flags.STR;
import static net.butfly.albatis.ddl.vals.ValType.Flags.STRL;
import static net.butfly.albatis.ddl.vals.ValType.Flags.UNKNOWN;

import java.time.Instant;
import java.util.Date;

import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.vals.ValType;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.format.RmapFormat;
import net.butfly.alserdes.SerDes;

@SerDes.As("cassandra")
public class CassandraFormat extends RmapFormat {
	private static final long serialVersionUID = 4733354000209088889L;

	@Override
	public Rmap ser(Rmap src) {
		Rmap r = src.skeleton();
		src.forEach((k, v) -> {
			Object bb = assemble(v, ValType.obj(v));
			if (null != bb) r.put(k, bb);
		});
		return r;
	}

	@Override
	public Rmap ser(Rmap src, FieldDesc... fields) {
		Rmap r = src.skeleton();
		Object b;
		for (FieldDesc f : fields) if (null != (b = assemble(src.get(f.name), f.type))) r.put(f.name, b);
		return r;
	}

	@Override
	public Rmap deser(Rmap dst) {
		throw new IllegalArgumentException("Disassemble from hbase need schema.");
	}

	@Override
	public Rmap deser(Rmap dst, FieldDesc... fields) {
		Object v;
		for (FieldDesc f : fields) if (null != (v = dst.get(f.name))) {
			try {
				if (null != (v = disassemble(v, f.type))){
					dst.put(f.name, v);
				} else {
					dst.remove(f.name);
				}
			} catch (IllegalArgumentException e) {
				logger().error("Disassemble failed. Field:[" + f.name + "]", e);
				throw e;
			}
		}
		return dst;
	}

	private Object disassemble(Object b, ValType t) {
		if (null == b) return null;
		try {
			switch (t.flag) {
			case BINARY:
				return b;
			case DATE:
				return Date.from((Instant) b);
			case INT:
			case LONG:
			case FLOAT:
			case DOUBLE:
			case BOOL:
			case CHAR:
			case STR:
			case STRL:
			case GEO:
			case JSON_STR:
			case UNKNOWN:
				return b;
			}
			// will not be invoked
			logger().warn("Unknown disassembling: [" + b.toString() + "] =>[" + t.toString() + "], the warn should not happened!!");
			return b;
		} catch (IllegalArgumentException e) {
			throw e;
		}
	}

	private Object assemble(Object v, ValType t) {
		if (null == v) return null;
		switch (t.flag) {
		case BINARY:
			return v;
		case DATE:
			return ((Date) v).toInstant();
		case INT:
		case LONG:
		case FLOAT:
		case DOUBLE:
		case BOOL:
		case CHAR:
		case STR:
		case STRL:
		case GEO:
		case JSON_STR:
		case UNKNOWN:
			return v;
		}
		// will not be invoked
		logger().warn("Unknown assembling: [" + v + " ] =>[" + t.toString() + "], the warn should not happened!!");
		return null;
	}
}
