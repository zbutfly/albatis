package net.butfly.albatis.bcp;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.utils.Texts;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.bcp.utils.Ftp;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static net.butfly.albacore.base.BizUnit.logger;
import static net.butfly.albatis.bcp.Props.*;


public class TaskDesc {
	public final String id;
	public final String name;
	public final String srcDsURI;
	public final String dstDsUri;
	public final String tableName;
	public final String tableDesc;
	public final List<FieldDesc> fields = Colls.list();
	public final FileDesc fd;

	TaskDesc(ResultSet rs) throws SQLException {
		super();
		this.id = rs.getString("ID");
		this.name = rs.getString("NAME");
		this.srcDsURI = rs.getString("SRC_DS_URI");
		this.dstDsUri = rs.getString("DST_DS_URI");
		this.tableName = rs.getString("SRC_TABLE_NAME");
		this.tableDesc = rs.getString("TABLE_NAME");
		this.fd = new FileDesc();
	}

	public static class FieldDesc {
		public final String fieldName;
		public final String dstName;
		public final String dstExpr;
		public final String comment;

		FieldDesc(ResultSet rs) throws SQLException {
			super();
			this.fieldName = rs.getString("SRC_NAME");
			this.dstName = rs.getString("DST_NAME");
			this.dstExpr = rs.getString("DST_EXPR");
			this.comment = rs.getString("COLUMN_NAME");
		}

		@Override
		public String toString() {
			return fieldName + ":name=" + dstName + ";expr=" + dstExpr + ";comment:" + comment;
		}
	}

	public class FileDesc {
		public final Path base;
		private final Path recFile;
		private final String recFilename;

		FileDesc() throws SQLException {
			super();
			this.recFilename = tableName + "-" + Texts.formatDate("yyyyMMddhhmmss", new Date()) + ".log";
			this.recFile = BCP_PATH_BASE.resolve(recFilename);
			this.base = confirmDir(BCP_PATH_BASE.resolve(tableName));
			confirmDir(BCP_PATH_ZIP.resolve(tableName));
		}

		public void rec(String fn, int count, Map<String, Integer> fieldCounts) throws IOException {
			String info = "表名-序号:" + fn + " 对应ZIP记录数:" + count + " 记录时间:" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
			try (FileWriter w = new FileWriter(recFile.toFile(), true); PrintWriter p = new PrintWriter(w);) {
				p.println(info + "\n" + fieldCounts.toString());
			}
			Exeter.of().submit(() -> upload(Props.FTP_RETRIES));
		}

		void upload(int retries) {
			boolean flag;
			int retry = 0;
			try (Ftp ftp = Ftp.connect(new URISpec(dstDsUri))) {
				if (null != ftp) {
					while (!(flag = ftp.uploadFile(recFilename, recFile)) && retry++ < retries);
					if (!flag && retry >= retries) logger.error("Ftp sent failed: " + recFile);
				}
			}
		}
	}
}
