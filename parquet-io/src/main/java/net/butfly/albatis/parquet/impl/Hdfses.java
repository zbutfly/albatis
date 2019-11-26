package net.butfly.albatis.parquet.impl;

import java.io.File;

import org.apache.hadoop.conf.Configuration;

import net.butfly.albacore.utils.Utils;

public final class Hdfses extends Utils {
	public static Configuration manualHadoopConfiguration(String uri) {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", uri);
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		return conf;
	}

	public static Configuration autoHadoopConfiguration() {
		Configuration conf = new Configuration();
		String env = System.getProperty("hadoop.home.dir");
		if (null == env) env = System.getenv("HADOOP_HOME");
		if (null != env) {
			java.nio.file.Path etc = java.nio.file.Paths.get(env).resolve("etc").resolve("hadoop");
			File etcf = etc.toFile();
			if (etcf.exists() && etcf.isDirectory()) for (String f : etcf.list()) {
				File ff = etc.resolve(f).toFile();
				int i = f.lastIndexOf('.');
				String ext = i >= 0 ? f.substring(i).toLowerCase() : null;
				if (ff.isFile() && ".xml".equals(ext)) conf.addResource(ff.getPath());
			}
		}
		return conf;
	}

}
