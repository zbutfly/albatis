package net.butfly.albatis.jdbc.dialect;

import com.alibaba.fastjson.JSON;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static net.butfly.albatis.ddl.vals.ValType.Flags.*;

@DialectFor(subSchema = "postgresql", jdbcClassname = "org.postgresql.Driver")
public class PostgresqlDialect extends Dialect {
    private static final String UPSERT_SQL_TEMPLATE = "INSERT INTO %s (%s) VALUES (%s) ON CONFLICT(%s) DO UPDATE SET %s";

    @Override
    protected void doUpsert(Connection conn, String table, String keyField, List<Rmap> records, AtomicLong count) {
        records.sort((m1, m2) -> m2.size() - m1.size());
        List<String> allFields = new ArrayList<>(records.get(0).keySet());
        List<String> nonKeyFields = allFields.stream().filter(f -> !f.equals(keyField)).collect(Collectors.toList()); // update fields

        String fieldnames = allFields.stream().collect(Collectors.joining(", "));
        String values = allFields.stream().map(f -> "?").collect(Collectors.joining(", "));
        String updates = nonKeyFields.stream().map(f -> f + " = EXCLUDED." + f).collect(Collectors.joining(", "));
        String sql = String.format(UPSERT_SQL_TEMPLATE, table, fieldnames, values, keyField, updates);

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
    public void tableConstruct(Connection conn, String table, TableDesc tableDesc, List<FieldDesc> fields) {
        StringBuilder sb = new StringBuilder();
        List<String> fieldSql = new ArrayList<>();
        for (FieldDesc field : fields)
            fieldSql.add(buildSqlField(tableDesc, field));
        try (Statement statement = conn.createStatement()) {
            List<Map<String, Object>> indexes = tableDesc.indexes;
            sb.append("create table ").append("\"").append(table).append("\"").append("(").append(String.join(",", fieldSql.toArray(new String[0]))).append(")");
            statement.addBatch(sb.toString());
            if (null != indexes) {
                for (int i = 0, len = indexes.size(); i < len; i++) {
                    StringBuilder createIndex = new StringBuilder();
                    Map indexMap = indexes.get(i);
                    String type = (String) indexMap.get("type");
                    String alias = (String) indexMap.get("alias");
                    List<String> fieldList = com.alibaba.fastjson.JSONArray.parseArray(JSON.toJSONString(indexMap.get("field")), String.class);
                    createIndex.append("create ").append(type).append(" ").append(alias).append(" on ").append("\"").append(table).append("\"").append("(").append("\"").append(String.join("\",\"", fieldList.toArray(new String[0]))).append("\"").append(")");
                    statement.addBatch(createIndex.toString());
                }
            }
            statement.executeBatch();
            logger().debug("execute ``````" + sb + "`````` success");
        } catch (SQLException e) {
            throw new RuntimeException("create postgre table failure " + e);
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    protected String buildSqlField(TableDesc tableDesc, FieldDesc field) {
        StringBuilder sb = new StringBuilder();
        switch (field.type.flag) {
            case INT:
            case SHORT:
            case BINARY:
                sb.append("\"").append(field.name).append("\"").append(" int4");
                break;
            case LONG:
                sb.append("\"").append(field.name).append("\"").append(" int8");
                break;
            case FLOAT:
            case DOUBLE:
                sb.append("\"").append(field.name).append("\"").append(" numeric(10)");
                break;
            case BYTE:
            case BOOL:
                sb.append("\'").append(field.name).append("\'").append(" bool(1)");
                break;
            case STR:
            case CHAR:
            case UNKNOWN:
                sb.append("\"").append(field.name).append("\"").append(" varchar(256)");
                break;
            case DATE:
                sb.append("\"").append(field.name).append("\"").append(" date");
                break;
            default:
                break;
        }
        if (tableDesc.keys.get(0).contains(field.name)) sb.append(" not null primary key");
        return sb.toString();
    }
}
