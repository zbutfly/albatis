package net.butfly.albatis.jdbc.dialect;

import net.butfly.albacore.exception.NotImplementedException;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.logger.Loggable;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

@DialectFor
public class Dialect implements Loggable {
    public static Dialect of(String schema) {
        String s = schema;
        Set<Class<? extends Dialect>> classes = Reflections.getSubClasses(Dialect.class, "net.butfly.albatis.jdbc.dialect");
        while (s.indexOf(":") > 0) {
            s = s.substring(s.indexOf(":") + 1);
            for (Class<? extends Dialect> c : classes)
                if (c.getAnnotation(DialectFor.class).subSchema().equals(s)) //
                    return Reflections.construct(c);
        }
        return new Dialect();
    }

    public String jdbcConnStr(URISpec uriSpec) {
        return uriSpec.toString();
    }

    public long upsert(Map<String, List<Rmap>> allRecords, Connection conn) {
        AtomicLong count = new AtomicLong();
        Exeter.of().join(entry -> {
            String table = entry.getKey();
            List<Rmap> records = entry.getValue();
            if (records.isEmpty()) return;
            String keyField = Dialects.determineKeyField(records);
            if (null != keyField) doUpsert(conn, table, keyField, records, count);
            else doInsertOnUpsert(conn, table, records, count);
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

    protected boolean tableExisted(Connection conn, String table) {
        throw new NotImplementedException();
    }
}
