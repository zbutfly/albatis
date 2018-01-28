package net.butfly.albatis.mongodb;

import java.io.IOException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.util.concurrent.AtomicDouble;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.lambda.Runnable;

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
		try (MongoConnection c = new MongoConnection(new URISpec("mongodb://hzga:hzga5678@127.0.0.1:30012/hzga"));
				MongoInput in = new MongoInput("TestMongoInput", c);) {
			in.table("gazhk_KDSJ_2015");
			for (int i = 0; i < parallelism; i++) {
				final int ii = i;
				new Thread(() -> {
					final NumberFormat nf = new DecimalFormat("0.00");
					final DateFormat df = new SimpleDateFormat("MM-dd hh:mm:ss ");
					long now = begin;
					while (!in.empty()) {
						AtomicLong count = new AtomicLong(), size = new AtomicLong();
						in.dequeue(s -> size.addAndGet(s.map(m -> {
							count.incrementAndGet();
							return m.toString().length();
						}).reduce((i1, i2) -> i1 + i2)));
						long curr = new Date().getTime();
						total.addAndGet(size.get() / 1024.0 / 1024);
						System.out.println(df.format(new Date()) + ii //
								+ "<count: " + count.get() + "> in <" + nf.format((curr - now) / 1000.0) + " secs>, "//
								+ "size: <" + nf.format(size.get() / 1024.0 / 1024) + " MByte>, "//
								+ "total: <" + nf.format(total.get() / 1024) + " Gbytes>, "//
								+ "avg: <" + nf.format(total.get() / ((curr - begin) / 1000.0)) + " MBytes/sec>, " //
						);
						now = new Date().getTime();
					}
				}, "TestMongoInputConsumer#" + i).start();
			}
			Runnable r = () -> {
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {}
			};
			r.until(() -> false).run();
		}
	}
}
