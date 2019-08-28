package net.butfly.albatis.bcp;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import net.butfly.albacore.utils.Configs;

import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

public class Config implements Closeable {
	private final HikariDataSource ds;
	private static final Logger LOGGER = Logger.getLogger(Config.class);
	private static Properties pro = new Properties();
	private static String MODE = Configs.gets("dataggr.migrate.config.prefix", "uniq");

	static {
		InputStream is = Config.class.getClassLoader().getResourceAsStream("bcpsql.properties");
		if (null != is) {
			try {
				pro.load(is);
			} catch (IOException e) {
				LOGGER.error("Loading config is failure", e);
			}
		}
	}

	public Config(String url, String usr, String psd) {
		HikariConfig config = new HikariConfig();
		config.setJdbcUrl(url);
		config.setUsername(usr);
		config.setPassword(psd);
		config.addDataSourceProperty("cachePrepStmts", "true");
		config.addDataSourceProperty("prepStmtCacheSize", "20");
		config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
		ds = new HikariDataSource(config);
	}

	public Connection getConnection() throws SQLException { return ds.getConnection(); }

	@Override
	public void close() {
		ds.close();
	}

	public List<TaskDesc> getTaskConfig(String taskname) throws SQLException {
		List<TaskDesc> configs = new LinkedList<>();
		String taskSql = pro.getProperty("dataggr.migrate.bcp.sql.task");
		LOGGER.trace("task sql is "+taskSql);
		try (Connection conn = getConnection(); PreparedStatement ps = conn.prepareStatement("uniq".equals(MODE) ? taskSql : SQL.SQL_TASK)) {
			ps.setString(1, taskname);
			if ("uniq".equals(MODE))
				ps.setString(2,taskname);
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) configs.add(new TaskDesc(rs));
			}
		}
		return configs;
	}

	public List<TaskDesc.FieldDesc> getTaskStatistics(String taskId) throws SQLException {
		List<TaskDesc.FieldDesc> fields = new LinkedList<>();
		String fieldSql = pro.getProperty("dataggr.migrate.bcp.sql.fields");
		LOGGER.trace("fields sql is "+fieldSql);
		try (Connection conn = getConnection(); PreparedStatement ps = conn.prepareStatement("uniq".equals(MODE) ? fieldSql : SQL.SQL_FIELDS)) {
			ps.setObject(1, taskId);
			try (ResultSet rs = ps.executeQuery();) {
				while (rs.next()) fields.add(new TaskDesc.FieldDesc(rs));
			}
		}
		return fields;
	}

	public List<TaskDesc> getTaskConfigInc(String taskname) throws SQLException {
		List<TaskDesc> configs = new LinkedList<>();
		try (Connection conn = getConnection(); PreparedStatement ps = conn.prepareStatement(SQL.SQL_TASK_INC);) {
			ps.setString(1, taskname);
			try (ResultSet rs = ps.executeQuery();) {
				while (rs.next()) configs.add(new TaskDesc(rs));
			}
		}
		return configs;
	}

	public List<TaskDesc.FieldDesc> getTaskStatisticsInc(String taskId) throws SQLException {
		List<TaskDesc.FieldDesc> fields = new LinkedList<>();
		try (Connection conn = getConnection(); PreparedStatement ps = conn.prepareStatement(SQL.SQL_FIELDS_INC);) {
			ps.setObject(1, taskId);
			try (ResultSet rs = ps.executeQuery();) {
				while (rs.next()) fields.add(new TaskDesc.FieldDesc(rs));
			}
		}
		return fields;
	}

	private static interface SQL {
		static String SQL_FIELDS_INC = "select \n"
				+ "\tsc.DB_COLUMN_NAME as SRC_NAME,sc.COLUMN_NAME, sc.IS_PK as SRC_KEY, ss.CONFIG_VALUE as SRC_TYPE,\n"
				+ "\tIF (LENGTH(sc.TIME_FORMAT) = 0, NULL, sc.TIME_FORMAT) as SRC_FORMAT, IF (LENGTH(stc.SPLITTER_TYPE) = 0, NULL, stc.SPLITTER_TYPE) as SRC_SPLITTER_TYPE,\n"
				+ "\tdc.DB_COLUMN_NAME as DST_NAME, dc.IS_PK as DST_KEY, IF(LENGTH(ds.CONFIG_VALUE)=0,NULL,ds.CONFIG_VALUE) as DST_TYPE,\n"
				+ "\tIF(LENGTH(dtc.TRANSFORM_FORMAT)=0,NULL,dtc.TRANSFORM_FORMAT) as DST_EXPR, IF(LENGTH(dc.TIME_FORMAT)=0,NULL,dc.TIME_FORMAT) as DST_FORMAT,\n"
				+ "\tIF(LENGTH(ssc.CONFIG_VALUE)=0,NULL,ssc.CONFIG_VALUE) as DST_SEG_MODE\n" + " from TASK_INFO t \n"
				+ "\tinner join TASK_TABLE_COLUMN_REL tc on t.FULL_TASK_ID = tc.TASK_ID \n"
				+ "\tinner join TASK_TABLE_COLUMN stc on tc.SOURCE_TASK_TABLE_COLUMN_ID = stc.TASK_TABLE_COLUMN_ID and stc.DELETE_FLAG = 0 \n"
				+ "\tinner join TABLE_COLUMN sc on stc.TABLE_COLUMN_ID = sc.TABLE_COLUMN_ID and sc.DELETE_FLAG = 0 \n"
				+ "\tinner join SYS_CODE_CONFIG ss on sc.COLUMN_TYPE = ss.ID and ss.DELETE_FLAG = 0 \n"
				+ "\tinner join SYS_CODE_CONFIG ss0 on ss.PARENT_ID = ss0.ID and ss0.CONFIG_CODE = 'C0003' and ss0.DELETE_FLAG = 0 \n"
				+ "\tinner join TASK_TABLE_COLUMN dtc on tc.TARGET_TASK_TABLE_COLUMN_ID = dtc.TASK_TABLE_COLUMN_ID and dtc.DELETE_FLAG = 0 \n"
				+ "\tinner join TABLE_COLUMN dc on dtc.TABLE_COLUMN_ID = dc.TABLE_COLUMN_ID and dc.DELETE_FLAG = 0 \n"
				+ "\tinner join SYS_CODE_CONFIG ds on dc.COLUMN_TYPE = ds.ID and ds.DELETE_FLAG = 0 \n"
				+ "\tinner join SYS_CODE_CONFIG ds0 on ds.PARENT_ID = ds0.ID and ds0.CONFIG_CODE = 'C0003' and ds0.DELETE_FLAG = 0 \n"
				+ "\tleft join SYS_CODE_CONFIG ssc on ssc.ID = dtc.ANALYZER and ssc.DELETE_FLAG = 0\n"
				+ " where t.TASK_ID = ? and t.TASK_TYPE = '1' and t.DELETE_FLAG = 0;";
		static String SQL_TASK_INC = "select ti.TASK_ID as ID, ti.TASK_NAME as NAME, \n"
				+ "\tsd.DATA_SOURCE_ID as SRC_DS_ID, sd.DATA_SOURCE_NAME as SRC_DS_NAME, \n"
				+ "\tsdst.CONFIG_VALUE as SRC_DS_TYPE, sd.CONNECTION_PARAM as SRC_DS_URI, \n"
				+ "\tst.TABLE_ID as SRC_TABLE_ID, tinc.TOPIC as SRC_TABLE_NAME, \n"
				+ "\tdd.DATA_SOURCE_ID as DST_DS_ID, dd.DATA_SOURCE_NAME as DST_DS_NAME, \n"
				+ "\tddst.CONFIG_VALUE as DST_DS_TYPE, dd.CONNECTION_PARAM as DST_DS_URI, \n"
				+ "\tdt.TABLE_ID as DST_TABLE_ID, dt.DB_TABLE_NAME as DST_TABLE_NAME ,dt.TABLE_NAME \n" + " from TASK_INFO ti  \n"
				+ "\tinner join TASK_INFO t on t.TASK_ID = ti.FULL_TASK_ID and t.DELETE_FLAG = 0 and t.TASK_TYPE = '0' \n"
				+ "\tinner join TASK_INCREMENT_TABLE tinc on tinc.TASK_ID = ti.TASK_ID \n"
				+ "\tinner join TASK_TABLE stt on t.TASK_ID = stt.TASK_ID and stt.IN_OUT_FLAG in (0, 2) and stt.DELETE_FLAG = 0 \n"
				+ "\tinner join DATA_SOURCE fsds on stt.DATA_SOURCE_ID = fsds.DATA_SOURCE_ID and fsds.IN_OUT_FLAG in (0, 2) and fsds.DELETE_FLAG = 0 \n"
				+ "\tinner join TABLE_INFO st on stt.TABLE_ID = st.TABLE_ID and st.DELETE_FLAG = 0 \n"
				+ "\tinner join DATA_SOURCE sd on tinc.DATA_SOURCE_ID = sd.DATA_SOURCE_ID and sd.IN_OUT_FLAG in (0, 2) and sd.DELETE_FLAG = 0 \n"
				+ "\tinner join SYS_CODE_CONFIG sdst on sd.DATA_SOURCE_TYPE = sdst.ID and sdst.DELETE_FLAG = 0  \n"
				+ "\tinner join TASK_TABLE dtt on t.TASK_ID = dtt.TASK_ID and dtt.IN_OUT_FLAG in (1, 2) and dtt.DELETE_FLAG = 0 \n"
				+ "\tinner join TABLE_INFO dt on dtt.TABLE_ID = dt.TABLE_ID and dt.DELETE_FLAG = 0 \n"
				+ "\tinner join DATA_SOURCE dd on dtt.DATA_SOURCE_ID = dd.DATA_SOURCE_ID and dd.IN_OUT_FLAG in (1, 2) and dd.DELETE_FLAG = 0 \n"
				+ "\tinner join SYS_CODE_CONFIG ddst on dd.DATA_SOURCE_TYPE = ddst.ID and ddst.DELETE_FLAG = 0 \n"
				+ " where ti.TASK_NAME like ? and ti.TASK_TYPE = '1' and ti.DELETE_FLAG = 0";
		static String SQL_TASK = "select t.TASK_ID as ID, t.TASK_NAME as NAME, \n"
				+ "\t sd.DATA_SOURCE_ID as SRC_DS_ID, sd.DATA_SOURCE_NAME as SRC_DS_NAME, \n"
				+ "\t sdst.CONFIG_VALUE as SRC_DS_TYPE, sd.CONNECTION_PARAM as SRC_DS_URI, \n"
				+ "\t st.TABLE_ID as SRC_TABLE_ID, st.DB_TABLE_NAME as SRC_TABLE_NAME,st.TABLE_NAME, \n"
				+ "\t dd.DATA_SOURCE_ID as DST_DS_ID, dd.DATA_SOURCE_NAME as DST_DS_NAME, \n"
				+ "\t ddst.CONFIG_VALUE as DST_DS_TYPE, dd.CONNECTION_PARAM as DST_DS_URI, \n"
				+ "\t dt.TABLE_ID as DST_TABLE_ID, dt.DB_TABLE_NAME as DST_TABLE_NAME \n" + " from TASK_INFO t \n"
				+ "\t inner join TASK_TABLE stt on t.TASK_ID = stt.TASK_ID and stt.DELETE_FLAG = 0 \n"
				+ "\t inner join TABLE_INFO st on stt.TABLE_ID = st.TABLE_ID and st.DELETE_FLAG = 0 \n"
				+ "\t inner join DATA_SOURCE sd on stt.DATA_SOURCE_ID = sd.DATA_SOURCE_ID and stt.IN_OUT_FLAG = 0 and sd.DELETE_FLAG = 0 \n"
				+ "\t inner join SYS_CODE_CONFIG sdst on sd.DATA_SOURCE_TYPE = sdst.ID and sdst.DELETE_FLAG = 0 \n"
				+ "\t inner join TASK_TABLE dtt on t.TASK_ID = dtt.TASK_ID and dtt.DELETE_FLAG = 0 \n"
				+ "\t inner join TABLE_INFO dt on dtt.TABLE_ID = dt.TABLE_ID and st.DELETE_FLAG = 0 \n"
				+ "\t inner join DATA_SOURCE dd on dtt.DATA_SOURCE_ID = dd.DATA_SOURCE_ID and dtt.IN_OUT_FLAG = 1 and dd.DELETE_FLAG = 0 \n"
				+ "\t inner join SYS_CODE_CONFIG ddst on dd.DATA_SOURCE_TYPE = ddst.ID and ddst.DELETE_FLAG = 0 \n"
				+ " where t.TASK_NAME = ? and t.TASK_TYPE = '0' and t.DELETE_FLAG = 0 and t.FULL_TASK_ID is null";
		static String SQL_FIELDS = "select \n"
				+ "\t sc.DB_COLUMN_NAME as SRC_NAME, sc.COLUMN_NAME, sc.IS_PK as SRC_KEY, ss.CONFIG_VALUE as SRC_TYPE,\n"
				+ "\t stc.TRANSFORM_FORMAT as SRC_EXPR, IF(LENGTH(sc.TIME_FORMAT)=0,NULL,sc.TIME_FORMAT) as SRC_FORMAT,\n"
				+ "\t dc.DB_COLUMN_NAME as DST_NAME, dc.IS_PK as DST_KEY, ds.CONFIG_VALUE as DST_TYPE,\n"
				+ "\t IF(LENGTH(dc.TIME_FORMAT)=0,NULL,dc.TIME_FORMAT) as DST_FORMAT, IF(LENGTH(dtc.TRANSFORM_FORMAT)=0,NULL,dtc.TRANSFORM_FORMAT) as DST_EXPR,\n"
				+ "\t IF(LENGTH(ssc.CONFIG_VALUE)=0,NULL,ssc.CONFIG_VALUE) as DST_SEG_MODE, IF(LENGTH(dtc.COPYTO_FIELDS)=0,NULL,dtc.COPYTO_FIELDS) as DST_COPY_TO,\n"
				+ "\t IF(LENGTH(dtc.VERIFY_EXPRESSION)=0,NULL,dtc.VERIFY_EXPRESSION) as DST_VALID_EXPR, dtc.INDEX_FLAG as DST_SEARCH\n"
				+ " from TASK_INFO t inner join TASK_TABLE_COLUMN_REL tc on t.TASK_ID = tc.TASK_ID \n"
				+ "\t inner join TASK_TABLE_COLUMN stc on tc.SOURCE_TASK_TABLE_COLUMN_ID = stc.TASK_TABLE_COLUMN_ID and stc.DELETE_FLAG = 0 \n"
				+ "\t inner join TABLE_COLUMN sc on stc.TABLE_COLUMN_ID = sc.TABLE_COLUMN_ID and sc.DELETE_FLAG = 0 \n"
				+ "\t inner join SYS_CODE_CONFIG ss on sc.COLUMN_TYPE = ss.ID and ss.DELETE_FLAG = 0 and ss.DELETE_FLAG = 0 \n"
				+ "\t inner join SYS_CODE_CONFIG ss0 on ss.PARENT_ID = ss0.ID and ss0.CONFIG_CODE = 'C0003' and ss0.DELETE_FLAG = 0 \n"
				+ "\t inner join TASK_TABLE_COLUMN dtc on tc.TARGET_TASK_TABLE_COLUMN_ID = dtc.TASK_TABLE_COLUMN_ID and dtc.DELETE_FLAG = 0 \n"
				+ "\t inner join TABLE_COLUMN dc on dtc.TABLE_COLUMN_ID = dc.TABLE_COLUMN_ID and dc.DELETE_FLAG = 0 \n"
				+ "\t inner join SYS_CODE_CONFIG ds on dc.COLUMN_TYPE = ds.ID and ds.DELETE_FLAG = 0 \n"
				+ "\t inner join SYS_CODE_CONFIG ds0 on ds.PARENT_ID = ds0.ID and ds0.CONFIG_CODE = 'C0003' and ds0.DELETE_FLAG = 0 \n"
				+ "\t left join SYS_CODE_CONFIG ssc on ssc.ID = dtc.ANALYZER and ssc.DELETE_FLAG = 0\n"
				+ " where t.TASK_ID = ? and t.TASK_TYPE = '0' and t.DELETE_FLAG = 0;";
	}
}
