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
import static net.butfly.albatis.ddl.vals.ValType.Flags.GEO_SHAPE;
import static net.butfly.albatis.ddl.vals.ValType.Flags.INT;
import static net.butfly.albatis.ddl.vals.ValType.Flags.JSON_STR;
import static net.butfly.albatis.ddl.vals.ValType.Flags.LONG;
import static net.butfly.albatis.ddl.vals.ValType.Flags.STR;
import static net.butfly.albatis.ddl.vals.ValType.Flags.UNKNOWN;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.ddl.Desc;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.vals.ValType;

public class MappingConstructor {
	protected final Logger logger = Logger.getLogger(MappingConstructor.class);
	public static final String DEFAULT_FULLTEXT_NAME = "fullText";
	public static final String CONFIG_PREFIX = "albatis.es.mapping.";
	private static final boolean FULLTEXT_ENABLED = Boolean.parseBoolean(gets(CONFIG_PREFIX + "fulltext.enabled", "false"));

	public final boolean includeAll;
	public final String analyzer;
	public final boolean dynamic;

	public MappingConstructor() {
		this(Maps.of());
	}

	public MappingConstructor(Map<String, Object> options) {
		this.includeAll = Boolean.valueOf(options.getOrDefault("includeAll", gets(CONFIG_PREFIX + "include.all", "false")).toString());
		this.dynamic = Boolean.valueOf(options.getOrDefault("dynamic", gets(CONFIG_PREFIX + "dynamic", "true")).toString());
		this.analyzer = options.getOrDefault("analyzer", gets(CONFIG_PREFIX + "text.analyzer", "standard")).toString();
	}

	public List<Map<String, Object>> templates() {
		List<Map<String, Object>> templates = new ArrayList<>();
		templates.add(of("integers", of("match_mapping_type", "long", "match", "*_i", "mapping", of("type", "integer"))));
		templates.add(of("longs", of("match_mapping_type", "long", "match", "*_l", "mapping", of("type", "long"))));
		templates.add(of("floats", of("match_mapping_type", "double", "match", "*_f", "mapping", of("type", "float"))));
		templates.add(of("doubles", of("match_mapping_type", "double", "match", "*_d", "mapping", of("type", "double"))));
		templates.add(of("dates", of("match_mapping_type", "date", "match", "*_dt", "mapping", of("type", "date"))));
		// changed
		templates.add(of("keyword_strings", of("match_mapping_type", "string", "match", "*_s", "mapping", of("type", "keyword"))));
		// new
		templates.add(of("analyzed_strings", of("match_mapping_type", "string", "match", "*_tcn", "mapping", fieldAnalyzer())));
		templates.add(of("geo_points", of("match_mapping_type", "string", "match", "*_rpt", "mapping", of("type", "geo_point"))));
		templates.add(of("blobs", of("match_mapping_type", "string", "match", "*_bin", "mapping", of("type", "binary"))));
		templates.add(of("booleans", of("match_mapping_type", "boolean", "match", "*_b", "mapping", of("type", "boolean"))));
		// from subject
		templates.add(of("strings", of("match_mapping_type", "string", "mapping", of("type", "keyword"))));
		templates.add(of("detected_dates", of("mapping", of("type", "date"), "match_mapping_type", "string", "match", "*_dt")));

		return templates;
	}

	public Map<String, Object> construct(FieldDesc... fields) {
		Map<String, Object> fulltexts = of();
		Map<String, Object> fa = fieldAnalyzer();
		Map<String, Object> props = of();
		for (FieldDesc f : fields) {
			boolean indexed = f.attr(Desc.INDEXED);
			Map<String, Object> fm = fieldType(f);
			if (!indexed) fm.put("index", false);

			if (FULLTEXT_ENABLED) {
				String[] c2s = f.attr(Desc.FULLTEXT, DEFAULT_FULLTEXT_NAME).split(",");
				fm.put("copy_to", c2s.length == 1 ? c2s[0] : c2s);
				for (String c : c2s) fulltexts.computeIfAbsent(c, k -> fa);
			}
			props.put(f.name, fm);
		}
		props.putAll(fulltexts);
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
		case GEO_SHAPE:
			return of("type", "geo_shape");
		case BINARY:
			return of("type", "binary");
		}
		throw new IllegalArgumentException();
	}

	public Map<String, Object> fieldAnalyzer() {
		return fieldAnalyzer(analyzer);
	}

	public Map<String, Object> fieldAnalyzer(String a) {
		return null == a ? of("type", "keyword") : of("type", "text", "analyzer", a);
	}
}
