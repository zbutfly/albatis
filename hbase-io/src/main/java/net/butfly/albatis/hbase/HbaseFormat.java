package net.butfly.albatis.hbase;

import static net.butfly.albatis.ddl.Builder.Qualifier.parse;
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

import java.nio.ByteBuffer;
import java.util.Date;

import org.apache.hadoop.hbase.util.Bytes;

import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.vals.ValType;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.format.RmapFormat;
import net.butfly.alserdes.SerDes;

@SerDes.As("hbase")
public class HbaseFormat extends RmapFormat {
	private static final long serialVersionUID = 4733354000209088889L;

	@Override
	public Rmap ser(Rmap src) {
		Rmap r = src.skeleton();
		src.forEach((k, v) -> {
			byte[] bb = assemble(v, ValType.obj(v));
			if (null != bb && bb.length > 0) r.put(k, bb);
		});
		return r;
	}

	@Override
	public Rmap ser(Rmap src, FieldDesc... fields) {
		Rmap r = src.skeleton();
		byte[] b;
		for (FieldDesc f : fields)
			if (null != (b = assemble(src.get(f.qualifier.column), f.type)) && b.length > 0) r.put(f.qualifier.column, b);
		return r;
	}

	@Override
	public Rmap deser(Rmap dst) {
		throw new IllegalArgumentException("Disassemble from hbase need schema.");
	}

	@Override
	public Rmap deser(Rmap dst, FieldDesc... fields) {
		Object v;
		for (FieldDesc f : fields) {
			for (String k : dst.keySet()) {
				String fn = parse(dst.table(), k).column;
				if (f.qualifier.column.equals(fn)) {
					v = dst.get(k);
					if (v instanceof byte[]) {
						v = disassemble((byte[]) v, f.type);
						if (null != v) dst.put(fn, v);
					} else if (v instanceof ByteBuffer) {
						v = disassemble(((ByteBuffer) v).array(), f.type);
						if (null != v) dst.put(fn, v);
					}
					break;
				}
			}
		}
		return dst;
	}

	// private static final ZoneOffset DEFAULT_OFFSET = OffsetDateTime.now().getOffset();

	private Object disassemble(byte[] b, ValType t) {
		if (null == b || b.length == 0) return null;
		switch (t.flag) {
		case BINARY:
			return b;
		case DATE:
			Long ms = Bytes.toLong(b);
			return null == ms || ms.longValue() <= 0 ? null : new Date(ms.longValue());
		case INT:
			switch (b.length) {
			case 1:
				return b[0];
			case 2:
				return Bytes.toShort(b);
			case 4:
				return Bytes.toInt(b);
			case 8:
				return Bytes.toLong(b);
			}
			return Bytes.toInt(b);
		case LONG:
			return Bytes.toLong(b);
		case FLOAT:
			return Bytes.toFloat(b);
		case DOUBLE:
			return Bytes.toDouble(b);
		case BOOL:
			return Bytes.toBoolean(b);
		case CHAR:
		case STR:
		case STRL:
		case GEO:
		case JSON_STR:
		case UNKNOWN:
			return Bytes.toString(b);
		}
		// will not be invoked
		logger().warn("Unknown disassembling: [" + b.length + " bytes] =>[" + t.toString() + "], the warn should not happened!!");
		return b;
	}

	private byte[] assemble(Object v, ValType t) {
		if (null == v) return null;
		switch (t.flag) {
		case BINARY:
			return (byte[]) v;
		case DATE:
			return Bytes.toBytes(((Date) v).getTime());
		case INT:
			return Bytes.toBytes((Integer) v);
		case LONG:
			return Bytes.toBytes((Long) v);
		case FLOAT:
			return Bytes.toBytes((Float) v);
		case DOUBLE:
			return Bytes.toBytes((Double) v);
		case BOOL:
			return Bytes.toBytes((Boolean) v);
		case CHAR:
		case STR:
		case STRL:
		case GEO:
		case JSON_STR:
		case UNKNOWN:
			return Bytes.toBytes((String) v);
		}
		// will not be invoked
		logger().warn("Unknown assembling: [" + v + " ] =>[" + t.toString() + "], the warn should not happened!!");
		return null;
	}
}
