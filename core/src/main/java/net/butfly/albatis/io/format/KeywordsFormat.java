package net.butfly.albatis.io.format;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Configs;
import net.butfly.albatis.io.Rmap;
import net.butfly.alserdes.SerDes;
import net.butfly.alserdes.SerDes.MapSerDes;

@SerDes.As("kwN")
@SerDes.As(value = "kwsN", list = true)
public class KeywordsFormat implements MapSerDes<Rmap> {


    private final String uriString = Configs.get("keywords.uri");
    private final String sqlString = Configs.get("keywords.getWordsql");
    private final String keyField = Configs.get("keywords.keyField");
    private final String WORDS = Configs.get("keywords.wordsField");
    HikariDataSource ds = null;
    @Override
    public Map<String, Object> deser(Rmap rmap) {
        return null == rmap ? null : rmap;
    }

    @Override
    public List<Map<String, Object>> desers(Rmap rmap) {
        List<Map<String, Object>> RMaps = new ArrayList<Map<String, Object>>();
        String textString = rmap.get(keyField).toString();
        Set<String> words = connect(textString);
        if(words.size()>0){
            for (String word : words) {
                Rmap rr = new Rmap();
                rr.table(rmap.table());
                rr.key(rmap.key());
                rr.putAll(rmap);
                rr.put(WORDS, word);
                RMaps.add(rr);
            }
            return RMaps;
        }else {
            return null;
        }

    }

    public void DataSource(String url, String usr, String psd) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(usr);
        config.setPassword(psd);
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "20");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        ds = new HikariDataSource(config);
    }

    public Set<String> connect(String testString) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet result = null;
        try {
            if(null==ds){
                URISpec uri = new URISpec(uriString);
                DataSource(uriString, uri.getParameter("user"), uri.getParameter("password"));
            }
            connection = ds.getConnection();
            ps = connection.prepareStatement(sqlString);
            ps.setString(1,testString);
            result = ps.executeQuery();
            Set<String> list = new HashSet<>();
            if (null != result) {
                while (result.next()) {
                    list.add(result.getString(1));
                }
            }
            return list;
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if(null!=result)result.close();
                if(null!=ps)ps.close();
                if(null!=connection)connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;

    }

}
