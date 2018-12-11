
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

@SuppressWarnings({ "deprecation", "unused" })
public class HdfsTest {
	private static final String HDFS = "hdfs://127.0.0.1:9000/";

	public HdfsTest(String hdfs, Configuration conf) {
		this.hdfsPath = hdfs;
		this.conf = conf;
	}

	public HdfsTest() {
		// TODO Auto-generated constructor stub
	}

	private String hdfsPath;
	private Configuration conf = new Configuration();

	public static void main(String[] args) throws IOException, URISyntaxException {
		HdfsTest hdfsgyt = new HdfsTest();
		String folder = HDFS + "mr/groom_system/small2.csv";
		String local = "/home/thinkgamer/Java/hadoop_shizhan/src/user_thing_tuijian/small2.csv";
		String local1 = "/home/thinkgamer/Java/hadoop_shizhan/src/user_thing_tuijian";
		// 判断某个文件夹是否存在
		// hdfsgyt.isExist(folder);
		// 创建文件夹
		// hdfsgyt.mkdir(folder);
		// 删除文件夹
		// hdfsgyt.rmr(folder);
		// 列出所有文件夹
		// hdfsgyt.ls(folder);
		// 递归列出所有文件夹
		// hdfsgyt.lsr(folder);
		// 上传文件
		// hdfsgyt.put(local, folder);
		// 下载文件
		// hdfsgyt.get(folder,local1);
		// 删除文件
		// hdfsgyt.rm(folder);
		// 显示文件
		// hdfsgyt.cat(folder);
	}

	// 显示文件
	private void cat(String folder) throws IOException, URISyntaxException {
		// 与hdfs建立联系
		FileSystem fs = FileSystem.get(new URI(HDFS), new Configuration());
		Path path = new Path(folder);
		FSDataInputStream fsdis = null;
		System.out.println("cat: " + folder);
		try {
			fsdis = fs.open(path);
			IOUtils.copyBytes(fsdis, System.out, 4096, false);
		} finally {
			IOUtils.closeStream(fsdis);
			fs.close();
		}
	}

	// 删除文件
	private void rm(String folder) throws IOException, URISyntaxException {
		// 与hdfs建立联系
		FileSystem fs = FileSystem.get(new URI(HDFS), new Configuration());
		Path path = new Path(folder);
		if (fs.deleteOnExit(path)) {
			fs.delete(path);
			System.out.println("delete:" + folder);
		} else {
			System.out.println("The fiel is not exist!");
		}
		fs.close();
	}

	// 下载文件
	private void get(String remote, String local) throws IllegalArgumentException, IOException, URISyntaxException {
		// 建立联系
		FileSystem fs = FileSystem.get(new URI(HDFS), new Configuration());
		fs.copyToLocalFile(new Path(remote), new Path(local));
		System.out.println("Get From :   " + remote + "   To :" + local);
		fs.close();
	}

	// 上传文件
	private void put(String local, String remote) throws IOException, URISyntaxException {
		// 建立联系
		FileSystem fs = FileSystem.get(new URI(HDFS), new Configuration());
		fs.copyFromLocalFile(new Path(local), new Path(remote));
		System.out.println("Put :" + local + "   To : " + remote);
		fs.close();
	}

	// 递归列出所有文件夹
	private void lsr(String folder) throws IOException, URISyntaxException {
		// 与hdfs建立联系
		FileSystem fs = FileSystem.get(new URI(HDFS), new Configuration());
		Path path = new Path(folder);
		// 得到该目录下的所有文件
		FileStatus[] fileList = fs.listStatus(path);
		for (FileStatus f : fileList) {
			System.out.printf("name: %s   |   folder: %s  |   size: %d\n", f.getPath(), f.isDir(), f.getLen());
			try {
				FileStatus[] fileListR = fs.listStatus(f.getPath());
				for (FileStatus fr : fileListR)
					System.out.printf("name: %s   |   folder: %s  |   size: %d\n", fr.getPath(), fr.isDir(), fr.getLen());
			} finally {}
		}
		fs.close();
	}

	// 列出所有文件夹
	private void ls(String folder) throws IOException, URISyntaxException {
		// 与hdfs建立联系
		FileSystem fs = FileSystem.get(new URI(HDFS), new Configuration());
		Path path = new Path(folder);
		// 得到该目录下的所有文件
		FileStatus[] fileList = fs.listStatus(path);
		for (FileStatus f : fileList) {
			System.out.printf("name: %s   |   folder: %s  |   size: %d\n", f.getPath(), f.isDir(), f.getLen());
		}
		fs.close();
	}

	// 删除文件夹
	private void rmr(String folder) throws IOException, URISyntaxException {
		// 与hdfs建立联系
		FileSystem fs = FileSystem.get(new URI(HDFS), new Configuration());
		Path path = new Path(folder);
		fs.delete(path);
		System.out.println("delete:" + folder);
		fs.close();
	}

	// 创建文件夹
	public void mkdir(String folder) throws IOException, URISyntaxException {
		// 与hdfs建立联系
		FileSystem fs = FileSystem.get(new URI(HDFS), new Configuration());
		Path path = new Path(folder);
		if (!fs.exists(path)) {
			fs.mkdirs(path);
			System.out.println("Create: " + folder);
		} else {
			System.out.println("it is have exist:" + folder);
		}
		fs.close();
	}

	// 判断某个文件夹是否存在
	private void isExist(String folder) throws IOException, URISyntaxException {
		// 与hdfs建立联系
		FileSystem fs = FileSystem.get(new URI(HDFS), new Configuration());
		Path path = new Path(folder);
		if (fs.exists(path)) {
			System.out.println("it is have exist:" + folder);
		} else {
			System.out.println("it is not exist:" + folder);
		}
		fs.close();
	}
}
