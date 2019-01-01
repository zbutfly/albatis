package net.butfly.albatis.io.format;

import java.util.List;

import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.logger.Loggable;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.io.Rmap;

public interface RmapFormatApi extends Loggable {
	/**
	 * <b>Schemaless</b> record assemble.
	 */
	Rmap assemble(Rmap src);

	/**
	 * <b>Schemaness</b> record assemble.<br>
	 * default as if schemaless
	 */
	default Rmap assemble(Rmap src, FieldDesc... fields) {
		return assemble(src); //
	}

	/**
	 * <b>Schemaless</b> record disassemble.
	 */
	Rmap disassemble(Rmap dst);

	/**
	 * <b>Schemaness</b> record disassemble.<br>
	 * default as if schemaless
	 */
	default Rmap disassemble(Rmap dst, FieldDesc... fields) {
		return disassemble(dst);
	}

	/**
	 * <b>Schemaless</b> record list assemble.
	 */
	default Rmap assembles(List<Rmap> l) {
		if (l.size() > 1) //
			logger().warn(getClass() + " does not support multiple serializing, only first will be writen: \n\t" + l.toString());
		Rmap first = l.get(0);
		return null == first ? null : assemble(first);
	}

	/**
	 * <b>Schemaless</b> record list disassemble.
	 */
	default List<Rmap> disassembles(Rmap m) {
		Rmap r = disassemble(m);
		return null == r || r.isEmpty() ? Colls.list() : Colls.list(r);
	}

	// TODO: List schemaness dis/assembling
}
