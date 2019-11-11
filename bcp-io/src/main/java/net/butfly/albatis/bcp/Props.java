package net.butfly.albatis.bcp;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Configs;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

import static net.butfly.albacore.utils.Configs.get;

public class Props {
	public static final String FIELD_SPLIT = "\t";
	public static final String ENCODING = "UTF-8";
	static {
		Configs.of(System.getProperty("file.conf", "file.properties"), "dataggr.migrate");
	}
	public static final int PARALLELISM = Integer.parseInt(get("parallelism", "30"));

	public static final int BCP_FLUSH_COUNT = Integer.parseInt(get("bcp.flush.count", "10000"));
	public static final int BCP_POOL_SIZE = Integer.valueOf(get("bcp.pool.size", Integer.toString(BCP_FLUSH_COUNT * 10)));
	public static final int BCP_FLUSH_MINS = Integer.parseInt(get("bcp.flush.idle.minutes", "30"));
	static final boolean BCP_FIELD_FULL_CONCURRENT = Boolean.parseBoolean(get("bcp.fields.concurrent", "true"));
	public static final Path BCP_PATH_BASE = confirmDir(Paths.get(get("bcp.path.base")));
	public static final Path BCP_PATH_ZIP = confirmDir(BCP_PATH_BASE.resolve("zip"));
	public static final int BCP_PARAL = Integer.valueOf(get("bcp.parallelism", "50"));
	public static final boolean BCP_KEY_FIELD_EXCLUDE = Boolean.valueOf(get("bcp.key.field.exclude","false"));
	public static long BCP_FILE_LISTEN_TIME = Long.valueOf(get("bcp.file.listen.time","30000")) ;
	public static long BCP_SLEEP_TIME = Long.valueOf(get("bcp.sleep.time","3000")) ;
	public static final boolean FTP_LOCAL_MODEL = Boolean.valueOf(get("ftp.local.model","false"));
	public static final int BCP_WAITING_TIME = Integer.parseInt(get("bcp.waiting.time", "100"));
	public static final URISpec FTP_URI;
	public static Path ftpPath;
	static {
		String f = get("ftp");
		FTP_URI = null == f ? null : new URISpec(f);
	}

	public static final int FTP_RETRIES = Integer.valueOf(get("ftp.retries", "5"));
	public static final int HTTP_RETRIES = Integer.valueOf(get("http.retries", "1"));

	public static final int HTTP_PARAL = Integer.valueOf(get("http.parallelism", "100"));
	public static final int HTTP_TIMEOUT = Integer.valueOf(get("http.timeout.ms", "30000"));
	public static final int HTTP_RCV_BUF_SIZE = Integer.valueOf(get("http.rcv.buffer.bytes", "100000"));
	public static final int HTTP_STATS_STEP = Integer.valueOf(get("http.stats.step", "10000"));

	public static final boolean CLEAN_TEMP_FILES = Boolean.parseBoolean(get("bcp.tempfields.clean", "true"));

	public static Path confirmDir(Path dir) {
		File file = dir.toFile();
		if (!file.exists() || !file.isDirectory()) file.mkdirs();
		return dir;
	}
}
