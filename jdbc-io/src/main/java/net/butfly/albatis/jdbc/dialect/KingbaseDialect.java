package net.butfly.albatis.jdbc.dialect;

import net.butfly.albatis.io.Rmap;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@DialectFor(subSchema = "kingbaseanalyticsdb", jdbcClassname = "com.kingbase.kingbaseanalyticsdb.Driver")
public class KingbaseDialect extends Dialect {
    private static final String UPSERT_SQL_TEMPLATE = "INSERT INTO %s (%s) VALUES (%s)";

    @Override
    protected void doInsertOnUpsert(Connection conn, String table, List<Rmap> records, AtomicLong count) {
        records.sort((m1, m2) -> m2.size() - m1.size());
        List<String> allFields = new ArrayList<>(records.get(0).keySet());

        String fieldnames = allFields.stream().collect(Collectors.joining(", "));
        String values = allFields.stream().map(f -> "?").collect(Collectors.joining(", "));
        String sql = String.format(UPSERT_SQL_TEMPLATE, table, fieldnames, values);

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            records.forEach(m -> {
                try {
                    for (int i = 0; i < allFields.size(); i++) {
                        Object value = m.get(allFields.get(i));
                        Dialects.setObject(ps, i + 1, value);
                    }
                    ps.addBatch();
                } catch (SQLException e) {
                    logger().warn(() -> "add `" + m + "` to batch error, ignore this message and continue.", e);
                }
            });
            int[] rs = ps.executeBatch();
            long sucessed = Arrays.stream(rs).filter(r -> r >= 0).count();
            count.addAndGet(sucessed);
        } catch (SQLException e) {
            logger().warn(() -> "execute batch(size: " + records.size() + ") error, operation may not take effect. reason:", e);
        }
    }

    @Override
    protected void doUpsert(Connection conn, String table, String keyField, List<Rmap> records, AtomicLong count) {
        records.forEach(r -> {
            String sql = "SELECT " + keyField + " FROM " + table + " where " + keyField + " = " + r.get(keyField);
            try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery();) {
                if (rs.next()) doUpdate(conn, keyField, table, r);
                else doInsert(conn, keyField, table, r);
            } catch (SQLException e) {
                logger().error("do upsert fail", e);
            }
        });
    }

    private void doUpdate(Connection conn, String k, String t, Rmap m) {
        StringBuilder sb = new StringBuilder("update ").append(t).append(" set ");
        for (Map.Entry<String, Object> entry : m.entrySet())
            if (!k.equals(entry.getKey())) //
                sb.append(entry.getKey()).append(" = ").append("'").append(entry.getValue()).append("'").append(",");
        sb.deleteCharAt(sb.length() - 1).append(" where ").append(k).append(" = ").append("'").append(m.get(k)).append("'");
        try (Statement ps = conn.createStatement();) {
            ps.executeUpdate(sb.toString());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void doInsert(Connection conn, String k, String t, Rmap m) {
        StringBuilder sbu = new StringBuilder("values (");
        StringBuilder sb = new StringBuilder("insert into ").append(t).append("( ");
        for (Map.Entry<String, Object> entry : m.entrySet()) {
            sb.append(entry.getKey()).append(" ,");
            sbu.append("'").append(entry.getValue()).append("'").append(",");
        }
        try (Statement ps = conn.createStatement();) {
            ps.executeUpdate(sb.deleteCharAt(sb.length() - 1).append(")").toString() //
                    + sbu.deleteCharAt(sbu.length() - 1).append(")").toString());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean tableExisted(Connection conn, String table) {
        String sql = "select * from pg_tables where schemaname = 'public' " + " and tablename = " + "'" + table + "'";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return true;
            }
        } catch (SQLException e) {
            logger().error("Judging whether table is exists that is failure",e);
        }
        return false;
    }
}