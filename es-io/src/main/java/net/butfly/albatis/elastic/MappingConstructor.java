package net.butfly.albatis.elastic;

import static net.butfly.albacore.utils.Configs.gets;
import static net.butfly.albacore.utils.collection.Maps.of;
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
import static net.butfly.albatis.ddl.vals.ValType.Flags.UNKNOWN;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.ddl.Desc;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.vals.ValType;

public class MappingConstructor {
	protected final Logger logger = Logger.getLogger(MappingConstructor.class);
	public static final String DEFAULT_FULLTEXT_NAME = "fullText";
	private static final String CONFIG_PREFIX = "albatis.es.mapping.";

	private final boolean includeAll;
	private String analyzer;
	private boolean dynamic;

	public MappingConstructor(Map<String, Object> options) {
		this.includeAll = Boolean.valueOf(options.getOrDefault("includeAll", gets(CONFIG_PREFIX + "include.all", "false")).toString());
		this.dynamic = Boolean.valueOf(options.getOrDefault("dynamic", gets(CONFIG_PREFIX + "dynamic", "true")).toString());
		this.analyzer = options.getOrDefault("analyzer", gets(CONFIG_PREFIX + "text.analyzer", "standard")).toString();
	}

	protected List<Map<String, Object>> templates() {
		List<Map<String, Object>> templates = new ArrayList<>();
		templates.add(of("booleans", // new
				of("match_mapping_type", "boolean", "match", "*_b", "mapping", of("type", "boolean"))));
		templates.add(of("integers", //
				of("match_mapping_type", "long", "match", "*_i", "mapping", of("type", "integer"))));
		templates.add(of("longs", //
				of("match_mapping_type", "long", "match", "*_l", "mapping", of("type", "long"))));
		templates.add(of("floats", //
				of("match_mapping_type", "double", "match", "*_f", "mapping", of("type", "float"))));
		templates.add(of("doubles", //
				of("match_mapping_type", "double", "match", "*_d", "mapping", of("type", "double"))));
		templates.add(of("dates", //
				of("match_mapping_type", "date", "match", "*_dt", "mapping", of("type", "date"))));
		templates.add(of("keyword_strings", // changed
				of("match_mapping_type", "string", "match", "*_s", "mapping", of("type", "keyword"))));
		templates.add(of("analyzed_strings", // new
				of("match_mapping_type", "string", "match", "*_tcn", "mapping", fieldAnalyzer())));
		templates.add(of("geo_points", // new
				of("match_mapping_type", "string", "match", "*_rpt", "mapping", of("type", "geo_point"))));
		templates.add(of("blobs", // new
				of("match_mapping_type", "string", "match", "*_bin", "mapping", of("type", "binary"))));
		return templates;
	}

	public Map<String, Object> construct(FieldDesc... fields) {
		Map<String, Object> props = of(DEFAULT_FULLTEXT_NAME, fieldAnalyzer());
		for (FieldDesc f : fields) {
			Map<String, Object> fm = fieldType(f);
			String c2 = f.attr(Desc.FULLTEXT);
			if (null != c2 && !c2.isEmpty()) {
				String[] c2s = c2.split(",");
				if (c2s != null && c2s.length != 0) fm.put("copy_to", c2s.length == 1 ? c2s[0] : c2s);
			}
			props.put(f.name, fm);
		}
		Map<String, Object> all = of("include_in_all", includeAll, "dynamic", String.valueOf(dynamic), "properties", props);
		if (dynamic) all.put("dynamic_templates", templates());
		return all;
	}

	protected Map<String, Object> fieldType(FieldDesc f) {
		switch ((null == f.type ? ValType.UNKNOWN : f.type).flag) {
		case BOOL:
			return of("type", "boolean");
		case INT:
			return of("type", "integer");
		case LONG:
			return of("type", "long");
		case FLOAT:
			return of("type", "float");
		case DOUBLE:
			return of("type", "double");
		case DATE:
			return of("type", "date");
		case UNKNOWN:
		case STR:
		case CHAR:
		case JSON_STR:
			// STRING输出字段，直接取 DPC 字段映射 SQL 结果里面的DST_SEG_MODE的值，如果不为NULL，那么加上analyzer这个分词器设定。
			// STRING输出字段，直接取 DPC 字段映射 SQL 结果里面的DST_SEG_MODE的值，如果为NULL，不需要加上analyzer这个分词器设定。
			return fieldAnalyzer(f.attr(Desc.SEGMODE));
		case GEO:
			return of("type", "geo_point");
		case BINARY:
			return of("type", "binary");
		}
		throw new IllegalArgumentException();
	}

	protected Map<String, Object> fieldAnalyzer() {
		return fieldAnalyzer(analyzer);
	}

	protected Map<String, Object> fieldAnalyzer(String a) {
		return null == a ? of("type", "keyword") : of("type", "text", "analyzer", a);
	}
}
