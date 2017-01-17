package net.butfly.albatis.mongodb;

import java.io.IOException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import com.google.common.util.concurrent.AtomicDouble;
import com.mongodb.DBObject;

import net.butfly.albacore.utils.async.Concurrents;

public class MongoIOTest {
	public static void main(String[] args) throws IOException {
		int parallelism;
		try {
			parallelism = Integer.parseInt(args[0]);
		} catch (Throwable t) {
			parallelism = 10;
		}

		final AtomicDouble total = new AtomicDouble(0);
		final long begin = new Date().getTime();// 10.118.159.44
		try (MongoInput in = new MongoInput("TestMongoInput", "mongodb://hzga:hzga5678@127.0.0.1:30012/hzga", "gazhk_KDSJ_2015",
				10000);) {
			for (int i = 0; i < parallelism; i++) {
				final int ii = i;
				new Thread(() -> {
					final NumberFormat nf = new DecimalFormat("0.00");
					final DateFormat df = new SimpleDateFormat("MM-dd hh:mm:ss ");
					long now = begin;
					List<DBObject> l;
					while ((l = in.dequeue(1000)).size() > 0) {
						long size = 0;
						for (DBObject m : l)
							size += m.toString().length();
						long curr = new Date().getTime();
						total.addAndGet(size / 1024.0 / 1024);
						System.out.println(df.format(new Date()) + ii //
								+ "<count: " + l.size() + "> in <" + nf.format((curr - now) / 1000.0) + " secs>, "//
								+ "size: <" + nf.format(size / 1024.0 / 1024) + " MByte>, "//
								+ "total: <" + nf.format(total.get() / 1024) + " Gbytes>, "//
								+ "avg: <" + nf.format(total.get() / ((curr - begin) / 1000.0)) + " MBytes/sec>, " //
						);
						now = new Date().getTime();
					}
				}, "TestMongoInputConsumer#" + i).start();
			}
			Concurrents.forever(() -> {
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {}
			}).run();
		}
	}
}
