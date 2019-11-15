package net.butfly.albatis.ddl;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;

/**
 * Desc of some models of one datasource (database).
 */
public final class DBDesc extends Desc<DBDesc> {
	private static final long serialVersionUID = 116728164544199423L;
	static final Logger logger = Logger.getLogger(DBDesc.class);
	private static final Map<String, DBDesc> models = Maps.of();

	public final String name;
	public final URISpec uri;
	// public final Map<String, Object> options = Maps.of();
	final Map<Qualifier, TableDesc> tables = Maps.of();

	public static void reg(DBDesc db) {
		models.put(db.name, db);
	}

	public static DBDesc db(String name) {
		return models.get(name);
	}

	// @SuppressWarnings("unchecked")
	// public static Map<String, DBDesc> of(Map<String, Map<String, Object>> modelsMap) {
	// Map<String, DBDesc> models = Maps.of();
	// if (empty(modelsMap)) return models;
	// for (String dbName : modelsMap.keySet())
	// if (!dbName.startsWith("//") && !dbName.startsWith(".")) {
	// Object v = modelsMap.get(dbName);
	// if (v instanceof CharSequence) v = Maps.of(".uri", v.toString());
	// models.put(dbName, new DBDesc(dbName, (Map<String, Object>) v));
	// }
	// return models;
	// }

	public static DBDesc of(String dbname, String dburi) {
		return models.computeIfAbsent(dbname, dbn -> new DBDesc(dbn, dburi));
	}

	// @SuppressWarnings("unchecked")
	// private DBDesc(String name, Map<String, Object> modelMap) {
	// super();
	// this.name = name;
	// Object v;
	// this.uri = null == (v = modelMap.remove(".uri")) ? null : new URISpec(v.toString());
	// if (null != (v = modelMap.remove(".options"))) attw((Map<String, Object>) v);
	// for (String t : modelMap.keySet()) {
	// v = modelMap.get(t);
	// if (t.startsWith(".")) logger.warn("Datasource [" + name + "] config map invalid option [" + t + "]: " + v);
	// else if (t.startsWith("//")) logger.debug("Datasource [" + name + "] config map comment [" + t + "]: " + v);
	// else tables.put(t, TableDesc.of(t, (Map<String, Object>) v));
	// }
	// reg(this);
	// }

	@Deprecated
	private DBDesc(String name, String uri) {
		this.name = name;
		this.uri = new URISpec(uri);
		this.attw(".rawuri", uri);
	}

	@Deprecated
	public DBDesc(String uri) {
		this(null, uri);
	}

	public Set<Qualifier> tables() {
		return tables.keySet();
	}

	@Deprecated
	public TableDesc table(String qualifier) {
		return tables.computeIfAbsent(new Qualifier(qualifier), q -> new TableDesc(q));
	}

	public TableDesc table(Qualifier qualifier) {
		return tables.computeIfAbsent(qualifier, q -> new TableDesc(q));
	}

	@Override
	public String toString() {
		return "[db:" + name + "[" + uri.toString() + "][tables:" + tables.size() + "]]" + super.toString();
	}

	public static String summary() {
		StringBuilder b = new StringBuilder();
		b.append("Models [").append(models.size()).append("] databases: ");
		for (DBDesc db : models.values()) {
			b.append("\n\t").append(db.name).append("[").append(db.uri).append("] with [").append(db.tables.size()).append("] tables");
			if (!db.tables.isEmpty()) b.append(": \n\t\t").append(db.tables.values().stream() //
					.map(t -> t.qualifier.name).collect(Collectors.joining(", ")));
		}
		return b.toString();

	}
}
