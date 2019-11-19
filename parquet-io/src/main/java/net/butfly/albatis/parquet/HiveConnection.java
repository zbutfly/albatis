package net.butfly.albatis.parquet;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.DataConnection;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableDesc;

/**
 * Supported uri:<br>
 * hive:parquet://.... (default same as file, means local file path)<br>
 * hive:parquet:file://....<br>
 * hive:parquet:hdfs://....<br>
 * hive:parquet:vfs://.... (unsupported now)<br>
 */
public class HiveConnection extends DataConnection<FileSystem> {
	private static final Logger logger = Logger.getLogger(HiveConnection.class);
	final static String schema = "hive";

	public Configuration conf;
	org.apache.hadoop.fs.Path base;

	public HiveConnection(URISpec urispec) throws IOException {
		super(urispec, schema);
	}

	@Override
	protected FileSystem initialize(URISpec uri) {
		List<String> schs = Colls.list(uri.getSchemas());

		if (!schs.isEmpty() && schema.equals(schs.get(0))) schs.remove(0); // ignore common "hive:"
		String sub = schs.isEmpty() ? "parquet" : schs.remove(0);
		switch (sub) {
		case "parquet":
			sub = schs.isEmpty() ? "file" : schs.remove(0);
			String path = uri.getPath();
			switch (sub) {
			case "file":
				this.base = new org.apache.hadoop.fs.Path(path);
				this.conf = null;
				break;
			case "hdfs":
				if (schs.isEmpty()) {// no hdfs position spec
					this.base = new org.apache.hadoop.fs.Path(path);
					this.conf = new Configuration();
					String host = uri.getHost();
					switch (host) {
					case "":// classpath
						logger.debug("HDFS configuration load from classpath.");
						conf.setClassLoader(HiveConnection.class.getClassLoader());
						break;
					case ".":// system
						logger.debug("HDFS configuration load from system environment/property.");
						String env = System.getProperty("hadoop.home.dir");
						if (null == env) env = System.getenv("HADOOP_HOME");
						if (null != env) {
							java.nio.file.Path etc = java.nio.file.Path.of(env).resolve("etc").resolve("hadoop");
							File etcf = etc.toFile();
							if (etcf.exists() && etcf.isDirectory()) for (String f : etcf.list()) {
								File ff = etc.resolve(f).toFile();
								int i = f.lastIndexOf('.');
								String ext = i >= 0 ? f.substring(i).toLowerCase() : null;
								if (ff.isFile() && ".xml".equals(ext)) conf.addResource(ff.getPath());
							}
						}
						break;
					default:
						logger.debug("HDFS configuration connect to server: " + host);
						conf.set("fs.defaultFS", "hdfs://" + host + this.base.toString());
						conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
						conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
					}
				} else switch (sub = schs.remove(0)) {
				case "file":
					String[] paths = path.split("~", 2);
					String confPath = paths[0];
					this.base = new org.apache.hadoop.fs.Path(paths.length > 1 ? paths[1] : "/");
					this.conf = new Configuration();
					logger.debug("HDFS configuration load from file: " + confPath);
					String env = System.getProperty("hadoop.home.dir");
					if (null == env) env = System.getenv("HADOOP_HOME");
					if (null != env) {
						java.nio.file.Path etc = java.nio.file.Path.of(env).resolve("etc").resolve("hadoop");
						File etcf = etc.toFile();
						if (etcf.exists() && etcf.isDirectory()) for (String f : etcf.list()) {
							File ff = etc.resolve(f).toFile();
							int i = f.lastIndexOf('.');
							String ext = i >= 0 ? f.substring(i).toLowerCase() : null;
							if (ff.isFile() && ".xml".equals(ext)) conf.addResource(ff.getPath());
						}
					}
					break;
				default:
					throw new IllegalArgumentException(sub + " not support in uri [" + uri + "] schema");
				}
				break;
			default:
				throw new IllegalArgumentException(sub + " not support in uri [" + uri + "] schema");
			}
			break;
		default:
			throw new IllegalArgumentException(sub + " not support in uri [" + uri + "] schema");
		}

		try {
			return null == conf ? null : FileSystem.get(conf);
		} catch (IOException e) {
			throw new IllegalArgumentException("fs connection failed.", e);
		}
	}

	@Override
	public void close() throws IOException {
		if (null != client) client.close();
		super.close();
	}

	public static class Driver implements net.butfly.albatis.Connection.Driver<HiveConnection> {
		static {
			DriverManager.register(new Driver());
		}

		@Override
		public HiveConnection connect(URISpec uriSpec) throws IOException {
			return new HiveConnection(uriSpec);
		}

		@Override
		public List<String> schemas() {
			return Colls.list(schema);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public HiveParquetInput inputRaw(TableDesc... table) throws IOException {
		return new HiveParquetInput("HiveParquetInput", this);
	}

	@SuppressWarnings("unchecked")
	@Override
	public HiveParquetOutput outputRaw(TableDesc... table) throws IOException {
		return new HiveParquetOutput("HiveParquetOutput", this, table);
	}

	@Override
	public void construct(String table, TableDesc tableDesc, List<FieldDesc> fields) {}
}
