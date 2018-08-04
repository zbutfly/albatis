package net.butfly.albatis.ddl;

import java.util.Map;
import java.util.stream.Collectors;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;

/**
 * Desc of some models of one datasource (database).
 */
public final class DBDesc extends Desc<DBDesc> {
	private static final long serialVersionUID = 116728164544199423L;
	private static final Logger logger = Logger.getLogger(DBDesc.class);
	private static final Map<String, DBDesc> models = Maps.of();

	public final String name;
	public final URISpec uri;
	// public final Map<String, Object> options = Maps.of();
	final Map<String, TableDesc> tables = Maps.of();

	public static void reg(DBDesc db) {
		models.put(db.name, db);
	}

	public static DBDesc db(String name) {
		return models.get(name.split("\\.", 2)[0]);
	}

	@SuppressWarnings("unchecked")
	public static Map<String, DBDesc> of(Map<String, Map<String, Object>> modelsMap) {
		Map<String, DBDesc> models = Maps.of();
		if (null == modelsMap || modelsMap.isEmpty()) return models;
		for (String dbName : modelsMap.keySet())
			if (!dbName.startsWith("//") && !dbName.startsWith(".")) {
				Object v = modelsMap.get(dbName);
				if (v instanceof CharSequence) v = Maps.of(".uri", v.toString());
				models.put(dbName, new DBDesc(dbName, (Map<String, Object>) v));
			}
		return models;
	}

	@SuppressWarnings("unchecked")
	private DBDesc(String name, Map<String, Object> modelMap) {
		super();
		this.name = name;
		Object v;
		this.uri = null == (v = modelMap.remove(".uri")) ? null : new URISpec(v.toString());
		if (null != (v = modelMap.remove(".options"))) attw((Map<String, Object>) v);
		for (String t : modelMap.keySet()) {
			v = modelMap.get(t);
			if (t.startsWith(".")) logger.warn("Datasource [" + name + "] config map invalid option [" + t + "]: " + v);
			else if (t.startsWith("//")) logger.debug("Datasource [" + name + "] config map comment [" + t + "]: " + v);
			else tables.put(t, TableDesc.of(t, (Map<String, Object>) v));
		}
		reg(this);
	}

	@Deprecated
	public DBDesc(String name, String uri) {
		this.name = name;
		this.uri = new URISpec(uri);
		reg(this);
	}

	@Deprecated
	public DBDesc(String uri) {
		this(null, uri);
	}

	public TableDesc table(String name) {
		return tables.get(name);
	}

	@Override
	public String toString() {
		return "DPC DB Desc [" + name + "]: [" + uri.toString() + "] with [" + tables.size() + "] tables: ";
	}

	public static String summary() {
		StringBuilder b = new StringBuilder();
		b.append("Models [").append(models.size()).append("] databases: ");
		for (DBDesc db : models.values()) {
			b.append("\n\t").append(db.name).append("[").append(db.uri).append("] with [").append(db.tables.size()).append("] tables");
			if (!db.tables.isEmpty()) b.append(": \n\t\t").append(db.tables.values().stream().map(t -> t.name + ": " + t.fields.size()
					+ " fields").collect(Collectors.joining(", ")));
		}
		return b.toString();

	}

	public TableDesc reg(TableDesc tbl) {
		if (null != tbl) tables.putIfAbsent(tbl.name, tbl);
		return tbl;
	}
}
