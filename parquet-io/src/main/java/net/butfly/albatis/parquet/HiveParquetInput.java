package net.butfly.albatis.parquet;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.parquet.io.InputFile;

import net.butfly.albacore.utils.Configs;
import net.butfly.albatis.io.OddInput;
import net.butfly.albatis.io.Rmap;

public class HiveParquetInput extends net.butfly.albacore.base.Namedly implements OddInput<Rmap> {
	private static final long serialVersionUID = -6888615156791286888L;
	private static final int parallelism = Integer.parseInt(Configs.get("albatis.hive.parquet.input.parallelism", "10"));
	private final BlockingQueue<ScanTask> TaskQueue = new LinkedBlockingQueue<>();

	private final HiveConnection conn;
	private InputFile infile;

	public HiveParquetInput(String name, HiveConnection conn) throws IOException {
		super(name);
		this.conn = conn;
		RemoteIterator<LocatedFileStatus> flist = conn.client.listFiles(conn.base, true);
		while (flist.hasNext()) {
			LocatedFileStatus f = flist.next();
			if (!f.isFile()) continue;
			Path p = f.getPath();
			String fn = p.getName();
			String ext = fn.substring(fn.lastIndexOf('.'));
			if (!"parquet".equals(ext)) continue;
			ScanTask task = new ScanTask(p);
		}
	}

	private class ScanTask implements net.butfly.albacore.lambda.Runnable {
		public ScanTask(Path p) {
			// TODO Auto-generated constructor stub
		}

		@Override
		public void run() {
			// TODO Auto-generated method stub
		}
	}

	@Override
	public Rmap dequeue() {
		return null;
	}

}
