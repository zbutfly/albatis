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
	/**
	 * name.cf:prefix#
	 */
	public final String fullname;
	public final Map<String, FieldDesc> fields = Maps.of();
	// options
	public final List<List<String>> keys = Colls.list();
	// extras, deprecated into options
	public Map<String, Object> construct = null;
	public boolean destruct = false;
	@Deprecated
	private String referTable;

	/**
	 * @param parent
	 * @param sub
	 *            cf:prefix#
	 */
	public static TableDesc of(TableDesc parent, String sub) {
		if (null == sub || sub.isEmpty()) return parent;
		TableDesc t = new TableDesc(parent.name + "." + sub, parent.destruct).attw(parent.attrs);
		if (null != parent.construct) {
			t.construct = Maps.of();
			t.construct.putAll(parent.construct);
		}
		t.referTable = parent.referTable;
		t.parse(t.fullname);
		String prefix = t.attr(Desc.COL_PREFIX);
		if (null != prefix) for (String fieldName : parent.fields.keySet())
			if (fieldName.startsWith(prefix)) t.fields.put(fieldName, parent.fields.get(fieldName));
		for (List<String> ks : parent.keys) {
			List<String> kk = Colls.list();
			for (String k : ks)
				if (t.fields.containsKey(k)) kk.add(k);
			if (!kk.isEmpty()) t.keys.add(kk);
		}
		return t;
	}

	public TableDesc(String fullname) {
		this(fullname, false);
	}

	private TableDesc(String fullname, boolean desctuct) {
		super();
		this.fullname = fullname;
		this.destruct = desctuct;
		this.name = parse(fullname);
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
		if (null != (v = opts.remove("keys"))) keys.addAll(TableDesc.parseKeys(v));
		destruct = null == (v = opts.remove("destruct")) ? false : Boolean.parseBoolean(v.toString());
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

	public FieldDesc field(String fieldName) {
		return fields.get(fieldName);
	}

	@Deprecated
	public Map<String, FieldDesc> fields() {
		return fields;
	}

	private String parse(String qf) {
		String name, cf = null, prefix = null;
		String[] s = qf.split("\\.", 2);
		name = s[0];
		if (s.length > 1) {
			s = s[1].split(":", 2);
			cf = s[0];
			if (s.length > 1) {
				prefix = s[1];
				if (prefix.endsWith("#")) prefix = prefix.substring(0, prefix.length() - 1);
			}
		}
		attw(Desc.COL_FAMILY, cf).attw(Desc.COL_PREFIX, prefix);
		return name;
	}

	@Override
	public String toString() {
		return "DPC Table [" + name + "] with [" + fields.size() + "] fields: ";
	}
}
