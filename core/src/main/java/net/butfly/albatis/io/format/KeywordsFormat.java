package net.butfly.albatis.io.format;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
public class KeywordsFormat implements MapSerDes<Rmap>{
	
	
	private  final String uriString = Configs.get("keywodrs.uri");
	private  final String sqlString = Configs.get("keywodrs.getWordsql");
	private  final String keyField = Configs.get("keywodrs.keyfield");
	private  final String IDsqlString= Configs.get("keywodrs.getIDsql");
	private  final String ID= Configs.get("keywodrs.IDField");
	private  final String WORDS= Configs.get("keywodrs.WORDSField");
	
	@Override
	public  List<Map<String, Object>> desers(Rmap rmap) {
		List<Map<String, Object>> RMaps = new ArrayList<Map<String,Object>>();
		String textString = rmap.get(keyField).toString();
		List<String> words = connect(textString);
		
		List<Map<String, Object>> listmap =connectsql();
		// rmap.get(Configs.gets(""))
		for(Map<String, Object> m: listmap) {
			if(words.contains(m.get(WORDS))) {
				Rmap rr = new Rmap();
				rr.table(rmap.table());
				rr.key(rmap.key());
				rr.putAll(rmap);
				rr.put(ID, m.get(ID));
				RMaps.add(rr);
			}
		}
		
		return RMaps;
		
	}
	
	public HikariDataSource DataSource(String url, String usr, String psd) {
		HikariConfig config = new HikariConfig();
		config.setJdbcUrl(url);
		config.setUsername(usr);
		config.setPassword(psd);
		config.addDataSourceProperty("cachePrepStmts", "true");
		config.addDataSourceProperty("prepStmtCacheSize", "20");
		config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
		HikariDataSource ds = new HikariDataSource(config);
		return ds;
	}
	public List<String> connect(String testString) {
		Connection connection = null;
		sqlString.replace("?", testString);
		try {
			URISpec uri = new URISpec(uriString);
			HikariDataSource dsDataSource = DataSource(uriString, 
					uri.getParameter("user"),uri.getParameter("password"));
			connection = dsDataSource.getConnection();
			PreparedStatement ps = connection.prepareStatement(sqlString);
			ResultSet result = ps.executeQuery();
			List<String> list = new ArrayList<>();

			if (null != result) {
				while (result.next()) {
					list.add(result.getString(0));
				}

			}
			return list;

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				connection.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return null;

}
	public List<Map<String, Object>> connectsql() {
		Connection connection = null;
		try {
			URISpec uri = new URISpec(uriString);
			HikariDataSource dsDataSource = DataSource(uriString, 
					uri.getParameter("user"),uri.getParameter("password"));
			connection = dsDataSource.getConnection();
			PreparedStatement ps = connection.prepareStatement(IDsqlString);
			ResultSet result = ps.executeQuery();
			List<Map<String, Object>> list = new ArrayList<>();

			if (null != result) {
				while (result.next()) {
					Map<String, Object> map = new ConcurrentHashMap<String, Object>();
					map.put(ID, result.getObject(ID));
					map.put(WORDS, result.getObject(WORDS));
					list.add(map);
				}

			}
			return list;

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				connection.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return null;

}
}
