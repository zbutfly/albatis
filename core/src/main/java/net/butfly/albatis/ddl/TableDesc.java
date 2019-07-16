package net.butfly.albatis.ddl;

import static net.butfly.albatis.ddl.DBDesc.logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import net.butfly.albacore.io.lambda.Consumer;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;

/**
 * Desc of some fields of one table.
 */
public class TableDesc extends Desc<TableDesc> {
	private static final long serialVersionUID = -5720312308026671887L;
	/**
	 * dbname.cf:prefix#
	 */
	public final Qualifier qualifier;
	// public final String name;
	private final Map<String, FieldDesc> fields = Maps.of();

	public final List<FieldDesc> fieldDescList = new ArrayList<>();
	// options
	public final List<List<String>> keys = Colls.list();
	public final List<Map<String, Object>> indexes = Colls.list();
	// extras, deprecated into options
	public Map<String, Object> construct = null;
	public boolean destruct = false;
	@Deprecated
	private String referTable;

	// @SuppressWarnings("unchecked")
	// public TableDesc of(Map<String, Object> config) {
	// return new TableDesc((String) config.remove(".name")).options((Map<String, Object>) config.remove(".options")).fields(config);
	// }

	// @SuppressWarnings("unchecked")
	// public static TableDesc of(String name, Map<String, Object> config) {
	// return new TableDesc(name).options((Map<String, Object>) config.remove(".options")).fields(config);
	// }

	TableDesc(Qualifier qualifier) {
		this(qualifier, false);
	}

	private TableDesc(Qualifier qualifier, boolean destruct) {
		super();
		this.destruct = destruct;
		this.qualifier = qualifier;
		// this.name = this.qualifier.table;
	}

	@SuppressWarnings("unchecked")
	public TableDesc options(Map<String, Object> opts) {
		Object v;
		if (null != (v = opts.remove("keys"))) keys.addAll(parseKeys(v));
		destruct = null == (v = opts.remove("destruct")) ? false : Boolean.parseBoolean(v.toString());
		if (null != (v = opts.remove("indexes"))) indexes.addAll(parseIndexes(v));
		referTable = null == (v = opts.remove("refer")) ? null : v.toString();
		if (null != (v = opts.remove("construct"))) {
			if (v instanceof Boolean) construct = Maps.of();
			else if (v instanceof Map) construct = (Map<String, Object>) v;
			else {
				logger.error("Invalid construct definition (should be Map<String, ?>): \n\t" + v
						+ "\n, ignored settings but enable construct.");
				construct = Maps.of();
			}
		}
		attw(opts);
		return this;
	}

	public void field(FieldDesc f) {
		fields.putIfAbsent(f.name, f);
		if (f.rowkey) {
			if (keys.isEmpty()) keys.add(Colls.list(f.name));
			else keys.get(0).add(f.name);
		}
	}

	public void field(Consumer<FieldDesc> using) {
		fields.values().forEach(using);
	}

	// public TableDesc fields(Map<String, Object> fieldMap) {
	// Object v;
	// for (String fieldName : fieldMap.keySet()) {
	// v = fieldMap.get(fieldName);
	// if (fieldName.startsWith(".")) logger.warn("Model [" + name + "] config map invalid option [" + fieldName + "]: " + v);
	// else if (fieldName.startsWith("//")) logger.debug("Model [" + name + "] config map comment [" + fieldName + "]: " + v);
	// else if (v instanceof CharSequence) fields.put(fieldName, Builder.field(this, fieldName, v.toString()));
	// else if (v instanceof Map) fields.put(fieldName, Builder.field(this, fieldName, name));
	// else logger.error("Model [" + name + "] config map invalid value [" + fieldName + "]: " + v);
	// }
	// return this;
	// }

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private List<Map<String, Object>> parseIndexes(Object v) {
		List<Map<String, Object>> indexes = Colls.list();
		if (null == v) return indexes;
		else if (v instanceof List) for (Object k : (List) v) {
			if (k instanceof Map) {
				Map<String, Object> index = (Map<String, Object>) k;
				indexes.add(index);
			} else if (k instanceof CharSequence) {
				ObjectMapper mapper = new ObjectMapper();
				Map<String, Object> index = Maps.of();
				try {
					index = mapper.readValue(k.toString(), mapper.getTypeFactory().constructMapType(Map.class, String.class, Object.class));
				} catch (IOException e) {
					logger.error("Parse Exception,Invalid indexes definition: " + k);
				}
				indexes.add(index);
			} else logger.error("Invalid index definition: " + k);
		}
		else logger.error("Invalid indexes definition: " + v);
		return indexes;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static List<List<String>> parseKeys(Object v) {
		List<List<String>> keys = Colls.list();
		if (null == v) return keys;
		if (v instanceof CharSequence) keys.add(Colls.list(v.toString()));
		else if (v instanceof List) for (Object k : (List) v) {
			if (k instanceof CharSequence) keys.add(Colls.list(k.toString()));
			else if (k instanceof List) keys.add((List<String>) k);
			else logger.error("Invalid key(s) definition: " + k);
		}
		else logger.error("Invalid key(s) definition: " + v);
		return keys;
	}

	public String rowkey() {
		if (keys.isEmpty()) return null;
		List<String> pk = keys.get(0);
		if (pk.isEmpty()) return null;
		return pk.get(0);
	}

	public List<String> colkey() {
		if (keys.size() < 2) return Colls.list();
		List<String> ck = Colls.list();
		for (int i = 1; i < keys.size(); i++) if (keys.get(i).size() > 1) return keys.get(1);
		else ck.add(keys.get(i).get(0));
		return ck;
	}

	public FieldDesc field(String name) {
		return fields.get(name);
	}

	public FieldDesc[] fields() {
		return fields.values().toArray(new FieldDesc[fields.size()]);
	}

	@Override
	public String toString() {
		return "[table:" + qualifier + "[fields:" + fields.size() + "]]" + super.toString();
	}

	public static TableDesc dummy(String qualifier) {
		return new TableDesc(new Qualifier(qualifier));
	}

	public TableDesc clone(String qualifier) {
		TableDesc t = new TableDesc(new Qualifier(qualifier), destruct);
		t.fields.putAll(fields);
		t.keys.addAll(keys);
		t.construct = construct;
		t.destruct = destruct;
		t.referTable = referTable;
		return t;
	}

	public static List<TableDesc> dummy(Map<String, String> keyMapping) {
		return Colls.list(keyMapping.entrySet(), e -> {
			TableDesc t = TableDesc.dummy(e.getKey());
			t.keys.add(Colls.list(e.getValue()));
			return t;
		});
	}

	public static List<TableDesc> dummy(String... table) {
		return dummy(Colls.list(table));
	}

	public static List<TableDesc> dummy(Iterable<String> table) {
		return Colls.list(table, TableDesc::dummy);
	}
}
