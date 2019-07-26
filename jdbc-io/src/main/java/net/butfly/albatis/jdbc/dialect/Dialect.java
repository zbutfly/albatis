package net.butfly.albatis.jdbc.dialect;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import net.butfly.albacore.exception.NotImplementedException;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.logger.Loggable;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;

@DialectFor
public class Dialect implements Loggable {
	public static Dialect of(String schema) {
		String s = schema.substring(schema.indexOf(':') + 1);
		Set<Class<? extends Dialect>> classes = Reflections.getSubClasses(Dialect.class,
				"net.butfly.albatis.jdbc.dialect");
		int lastpos;
		do {
			for (Class<? extends Dialect> c : classes)
				if (c.getAnnotation(DialectFor.class).subSchema().equals(s)) //
					return Reflections.construct(c);
		} while ((lastpos = s.lastIndexOf(':')) >= 0 && !(s = s.substring(0, lastpos)).isEmpty());
		return new Dialect();
	}

	public String jdbcConnStr(URISpec uriSpec) {
		StringBuilder url = new StringBuilder();
		if (uriSpec.toString().contains("?")) {
			url.append(uriSpec.getSchema()).append("://").append(uriSpec.getHost()).append("/")
					.append(uriSpec.getFile()).append("?").append(uriSpec.toString().split("\\?")[1]);
			URISpec uri = new URISpec(url.toString());
			return uri.toString();
		} else {
			url.append(uriSpec.getSchema()).append("://").append(uriSpec.getHost()).append("/")
					.append(uriSpec.getFile());
			URISpec uri = new URISpec(url.toString());
			return uri.toString();
		}
	}

	public long upsert(Map<String, List<Rmap>> allRecords, Connection conn) {
		AtomicLong count = new AtomicLong();
		Exeter.of().join(entry -> {
			String table = entry.getKey();
			List<Rmap> records = entry.getValue();
			if (records.isEmpty())
				return;
			String keyField = null != records.get(0).keyField() ? records.get(0).keyField()
					: Dialects.determineKeyField(records);
			if (null != keyField)
				doUpsert(conn, table, keyField, records, count);
			else
				doInsertOnUpsert(conn, table, records, count);

		}, allRecords.entrySet());
		return count.get();
	}

	protected void doInsertOnUpsert(Connection conn, String t, List<Rmap> l, AtomicLong count) {
		throw new NotImplementedException();
	}

	protected void doUpsert(Connection conn, String t, String keyField, List<Rmap> l, AtomicLong count) {
		throw new NotImplementedException();
	}

	public void tableConstruct(Connection conn, String table, TableDesc tableDesc, List<FieldDesc> fields) {
		throw new NotImplementedException();
	}

	protected String buildSqlField(TableDesc tableDesc, FieldDesc field) {
		throw new NotImplementedException();
	}

	public boolean tableExisted(Connection conn, String table) {
		throw new NotImplementedException();
	}

	public void alterColumn(Connection conn, String table, TableDesc tableDesc, List<FieldDesc> fields) {
		throw new NotImplementedException();
	}

	public List<Map<String, Object>> getResultListByCondition(Connection conn, String table,
			Map<String, Object> condition) {
		throw new NotImplementedException();
	}

	public void deleteByCondition(Connection conn, String table, Map<String, Object> condition) {
		throw new NotImplementedException();
	}

	public String buildCreateTableSql(String table, FieldDesc... fields) {
		throw new NotImplementedException();
	}
}
