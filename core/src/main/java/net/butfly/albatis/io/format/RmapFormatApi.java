//package net.butfly.albatis.io.format;
//
//import net.butfly.albatis.ddl.FieldDesc;
//import net.butfly.albatis.io.Rmap;
//import net.butfly.alserdes.format.FormatApi;
//
//public interface RmapFormatApi extends FormatApi<Rmap> {
//	/**
//	 * <b>Schemaness</b> record assemble.<br>
//	 * default as if schemaless
//	 */
//	default Rmap ser(Rmap src, FieldDesc... fields) {
//		return ser(src); //
//	}
//
//	/**
//	 * <b>Schemaness</b> record disassemble.<br>
//	 * default as if schemaless
//	 */
//	default Rmap deser(Rmap dst, FieldDesc... fields) {
//		return deser(dst);
//	}
//
//	// TODO: List schemaness dis/assembling
//}
