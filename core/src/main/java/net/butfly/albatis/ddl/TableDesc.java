package net.butfly.albatis.ddl;

import java.util.List;
import java.util.Map;

import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;

/**
 * Desc of some fields of one table.
 */
public class TableDesc extends Desc<TableDesc> {
	private static final Logger logger = Logger.getLogger(TableDesc.class);
	public final String name;
	public final Map<String, FieldDesc> fields = Maps.of();
	// private final Map<String, Object> options = Maps.of();
	// options
	public final List<List<String>> keys = Colls.list();
	// extras, deprecated into options
	public Map<String, Object> construct;
	public boolean destruct;
	private String referTable;

	TableDesc(TableDesc parent, String prefix) {
		name = parent.name + "." + prefix;
		attw(parent.attrs);
		construct = parent.construct;
		destruct = parent.destruct;
		referTable = parent.referTable;
		for (String fieldName : parent.fields.keySet())
			if (fieldName.startsWith(prefix)) fields.put(fieldName, parent.fields.get(fieldName));
		for (List<String> ks : parent.keys) {
			List<String> kk = Colls.list();
			for (String k : ks)
				if (fields.containsKey(k)) kk.add(k);
			if (!kk.isEmpty()) keys.add(kk);
		}
	}

	public TableDesc(String name) {
		super();
		this.name = name;
	}

	@SuppressWarnings("unchecked")
	public TableDesc of(Map<String, Object> config) {
		return new TableDesc((String) config.remove(".name")).options((Map<String, Object>) config.remove(".options")).fields(config);
	}

	@SuppressWarnings("unchecked")
	public static TableDesc of(String name, Map<String, Object> config) {
		return new TableDesc(name).options((Map<String, Object>) config.remove(".options")).fields(config);
	}

	@SuppressWarnings("unchecked")
	public TableDesc options(Map<String, Object> opts) {
		Object v;
		attw(opts);

		v = attr(Desc.TABLE_KEYS);
		if (null != v) keys.addAll(TableDesc.parseKeys(v));

		v = attr(Desc.TABLE_CONSTRUCT);
		if (null == v) construct = null;
		else {
			if (v instanceof Boolean) construct = Maps.of();
			else if (v instanceof Map) construct = (Map<String, Object>) v;
			else {
				logger.error("Invalid construct definition (should be Map<String, ?>): \n\t" + v
						+ "\n, ignored settings but enable construct.");
				construct = Maps.of();
			}
		}

		v = attr(Desc.TABLE_DESTRUCT);
		destruct = null == v ? false : Boolean.parseBoolean(v.toString());

		v = attr(Desc.TABLE_REFER);
		referTable = null == v ? null : v.toString();
		return this;
	}

	public TableDesc fields(Map<String, Object> fieldMap) {
		Object v;
		for (String fieldName : fieldMap.keySet()) {
			v = fieldMap.get(fieldName);
			if (fieldName.startsWith(".")) logger.warn("Model [" + name + "] config map invalid option [" + fieldName + "]: " + v);
			else if (fieldName.startsWith("//")) logger.debug("Model [" + name + "] config map comment [" + fieldName + "]: " + v);
			else if (v instanceof CharSequence) fields.put(fieldName, Builder.field(fieldName, v.toString()));
			else if (v instanceof Map) fields.put(fieldName, Builder.field(fieldName, name));
			else logger.error("Model [" + name + "] config map invalid value [" + fieldName + "]: " + v);
		}
		return this;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	static List<List<String>> parseKeys(Object v) {
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

	public List<String> rowkey() {
		return keys.isEmpty() ? Colls.list() : keys.get(0);
	}

	public List<String> colkey() {
		if (keys.size() < 2) return Colls.list();
		List<String> ck = Colls.list();
		for (int i = 1; i < keys.size(); i++)
			if (keys.get(i).size() > 1) return keys.get(1);
			else ck.add(keys.get(i).get(0));
		return ck;
	}

	FieldDesc field(String fieldName) {
		return fields.get(fieldName);
	}

	public TableDesc(String name, boolean construct, boolean destruct) {
		this.name = name;
		this.construct = construct ? Maps.of() : null;
		this.destruct = destruct;
		referTable = null;
	}

	@Deprecated
	public Map<String, FieldDesc> fields() {
		return fields;
	}

	@Override
	public String toString() {
		return "DPC Table [" + name + "] with [" + fields.size() + "] fields: ";
	}
}
