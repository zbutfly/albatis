package net.butfly.albatis.jdbc;

import net.butfly.albacore.io.URISpec;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PostgreTest {

    private static final Logger LOGGER = Logger.getLogger(PostgreTest.class);

    public static void main(String[] args) throws IOException, SQLException {
        JdbcConnection connection = new JdbcConnection(new URISpec("jdbc:postgresql://10.19.173.24:5432/dw?user=postgres&password=080940ef07b0b507cC"));
        Connection conn = connection.client.getConnection();
//        conn.setAutoCommit(false);
        PreparedStatement stat = conn.prepareStatement("select * from device_info");
//        stat.setFetchDirection(ResultSet.FETCH_FORWARD);
//        stat.setFetchSize(100);
        long now = System.currentTimeMillis();
        try {
            ResultSet resultSet = stat.executeQuery();
            LOGGER.debug("Query spent: " + (System.currentTimeMillis() - now) + " ms.");
            boolean next = resultSet.next();
            parseMeta(resultSet);
        } catch (SQLException e) {

        }

    }

    private static void parseMeta(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int colCount = meta.getColumnCount();
        String[] colNames = new String[colCount];
        for (int i = 0; i < colCount; i++) {
            int j = i + 1;
            colNames[i] = meta.getColumnLabel(j);
            int ctype = meta.getColumnType(j);
            LOGGER.debug("ResultSet col[" + j + ":" + colNames[i] + "]: " + meta.getColumnClassName(j) + "/" + meta.getColumnTypeName(j)
                    + "[" + ctype + "]");
        }
    }

    private static List<Map<String, Object>> getResultSet(ResultSet rs) {
        List<Map<String, Object>> mapList = new ArrayList<>();
        ResultSetMetaData rsmd = null;
        try {
            while (rs.next()) {
                Map<String, Object> map = new HashMap<>();
                rsmd = rs.getMetaData();
                int count = rsmd.getColumnCount();
                for (int i = 1; i <= count; i++) {
                    String key = rsmd.getColumnLabel(i);
                    Object value = rs.getObject(i);
                    map.put(key, value);
                }
                mapList.add(map);
            }
        } catch (SQLException e) {
        }
        return mapList;
    }
}
