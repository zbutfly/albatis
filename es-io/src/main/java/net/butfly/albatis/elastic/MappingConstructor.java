package net.butfly.albatis.elastic;

import static net.butfly.albacore.utils.collection.Maps.of;
import static net.butfly.albatis.ddl.vals.ValType.Flags.BINARY;
import static net.butfly.albatis.ddl.vals.ValType.Flags.BOOL;
import static net.butfly.albatis.ddl.vals.ValType.Flags.CHAR;
import static net.butfly.albatis.ddl.vals.ValType.Flags.DATE;
import static net.butfly.albatis.ddl.vals.ValType.Flags.DOUBLE;
import static net.butfly.albatis.ddl.vals.ValType.Flags.FLOAT;
import static net.butfly.albatis.ddl.vals.ValType.Flags.GEO;
import static net.butfly.albatis.ddl.vals.ValType.Flags.INT;
import static net.butfly.albatis.ddl.vals.ValType.Flags.LONG;
import static net.butfly.albatis.ddl.vals.ValType.Flags.STR;
import static net.butfly.albatis.ddl.vals.ValType.Flags.UNKNOWN;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.ddl.Desc;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.vals.ValType;

@SuppressWarnings("deprecation")
public class MappingConstructor {
	protected final Logger logger = Logger.getLogger(MappingConstructor.class);
	public static final String DEFAULT_FULLTEXT_NAME = "fullText";

	protected List<Map<String, Object>> templates() {
		List<Map<String, Object>> templates = new ArrayList<>();
		templates.add(of("integers", of("mapping", mapFullText("type", "integer", null), "match_mapping_type", "string", "match", "*_i")));
		templates.add(of("longs", of("mapping", mapFullText("type", "long", null), "match_mapping_type", "string", "match", "*_l")));
		templates.add(of("floats", of("mapping", mapFullText("type", "float", null), "match_mapping_type", "string", "match", "*_f")));
		templates.add(of("doubles", of("mapping", mapFullText("type", "double", null), "match_mapping_type", "string", "match", "*_d")));
		templates.add(of("dates", of("mapping", mapFullText("type", "date", null), "match_mapping_type", "date", "match", "*_dt")));
		templates.add(of("strings", of("mapping", mapFullText("type", "String", null), "match", "*_s")));
		return templates;
	}

	public Map<String, Object> construct(FieldDesc... fields) {
		Map<String, Object> props = of(DEFAULT_FULLTEXT_NAME, of("type", "text"));
		for (FieldDesc f : fields)
			suffix(props, f.name, f.type, f.attr(Desc.FULLTEXT));
		return of("include_in_all", true, "dynamic", true, "dynamic_templates", templates(), "properties", props);
	}

	protected void suffix(Map<String, Object> mapping, String fieldName, ValType dstType, String dstFullText) {
		switch ((null == dstType ? ValType.UNKNOWN : dstType).flag) {
		case DATE:
			mapping.put(fieldName, mapFullText("type", "date", dstFullText));
			return;
		case UNKNOWN:
		case STR:
		case CHAR:
			Map<String, Object> t = mapFullText("type", "text", dstFullText);
			t.put("fields", of("keyword", of("type", "keyword")));
			mapping.put(fieldName, t);
			return;
		case GEO:
			mapping.put(fieldName, mapFullText("type", "geo_point", dstFullText));
			return;
		case INT:
			mapping.put(fieldName, mapFullText("type", "integer", dstFullText));
			return;
		case LONG:
			mapping.put(fieldName, mapFullText("type", "long", dstFullText));
			return;
		case FLOAT:
			mapping.put(fieldName, mapFullText("type", "float", dstFullText));
			return;
		case DOUBLE:
			mapping.put(fieldName, mapFullText("type", "double", dstFullText));
			return;
		case BINARY:
		case BOOL:
			logger.warn("FieldMapping parse fail for type in config: " + dstType);
			return;
		}
	}

	protected Map<String, Object> mapFullText(String k, Object v, String fullTexts) {
		Map<String, Object> map = of(k, v);
		if (null == fullTexts) return map;
		String[] fts = fullTexts.split(",");
		if (fts == null || fts.length == 0) return map;
		if (fts.length == 1) map.put("copy_to", fts[0]);
		map.put("copy_to", fts);
		return map;
	}

}
