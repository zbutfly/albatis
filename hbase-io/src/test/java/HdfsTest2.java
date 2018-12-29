
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsTest2 {
	// hdfs
	static Configuration dconfig = null;
	static FileSystem dfs = null;
	// local
	static Configuration lconfig = null;
	static FileSystem lfs = null;

	public HdfsTest2() throws IOException {
		// hdfs conf
		dconfig = getConfiguration("/usr/local/hadoop/etc/hadoop");
		dfs = FileSystem.get(dconfig);
		// local conf
		lconfig = getConfiguration(null);
		lfs = FileSystem.get(lconfig);
	}

	public static Configuration getConfiguration(String path) throws IOException {
		// hdfs conf
		dconfig = new Configuration();
		if (null == path) return dconfig;
		dconfig.addResource(new Path(path + "/core-site.xml"));
		dconfig.addResource(new Path(path + "/hdfs-site.xml"));
//		org.apache.hadoop.fs.FileSystem afs;
		dconfig.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		dconfig.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		return dconfig;
	}

	public static void main(String... a) throws IOException {
		new HdfsTest2();
		createDir("/user/hduser/abcd");
		// deleteDir("/user/hduser/img");
		// copyFromLocalToHdfs("/home/hduser/Desktop/video.mp4", "/user/hduser/video/");
	}

	public static void createDir(String dirName) throws IOException {
		System.out.println(dfs.getWorkingDirectory());
		if (dfs.exists(new Path(dirName))) {
			System.out.println("directory already exists - " + dirName);
		} else {
			Path src = new Path(dirName);
			dfs.mkdirs(src);
			System.out.println("directory created- " + dirName);
		}
	}

	public static void deleteDir(String dirName) throws IOException {
		if (dfs.exists(new Path(dirName))) {
			dfs.delete(new Path(dirName), true);
			System.out.println("directory deleted- " + dirName);
		} else {
			System.out.println("directory doesnt exists - " + dirName);
		}
	}

	public static boolean copyFromLocalToHdfs(String localfile, String hdfsfile, Configuration configuration) throws IOException {
		// local
		Configuration conf = new Configuration();
		FileSystem localFileSystem = FileSystem.getLocal(conf);
		Path src = localFileSystem.makeQualified(new Path(localfile));
		// hdfs
		Configuration config = configuration;
		FileSystem dfs = FileSystem.get(config);
		if (dfs.exists(new Path(hdfsfile))) {
			dfs.delete(new Path(hdfsfile), true);
		}
		if (localFileSystem.exists(src)) {
			Path dst = new Path(hdfsfile);
			dfs.copyFromLocalFile(src, dst);
			return true;
		} else {
			System.out.println("local directory doesnt exists - " + localfile);
			return false;
		}
	}
}