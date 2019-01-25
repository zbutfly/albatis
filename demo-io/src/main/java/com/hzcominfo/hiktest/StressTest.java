package com.hzcominfo.hiktest;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.Schema;

import com.hikvision.multidimensional.kafka.register.schema.KafkaDemo;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.IOs;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.Connection;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.kafka.avro.AvroFormat;
import net.butfly.albatis.kafka.avro.SchemaReg;

public class StressTest {
	private static Logger logger = Logger.getLogger(StressTest.class);
	private static AtomicLong count = new AtomicLong(), spent = new AtomicLong();
	private static Schema schema = load();
	private static int paral = Integer.parseInt(System.getProperty("dataggr.migrate.kafka.stress.paral", "-1"));
	private static long limit = 5 * 10000 * 10000;
	private static final ExecutorService ex = ex();

	public static void main(String[] args) throws IOException {
		System.setProperty(SchemaReg.SCHEMA_REGISTRY_URL_CONF, KafkaDemo.SCHEMA_REG);
		TableDesc dst = AvroFormat.Builder.schema(KafkaDemo.TOPIC, schema);

		try (Connection conn = Connection.connect(new URISpec("kafka2:bootstrap://" + KafkaDemo.BOOTSTRAP + "?df=avro"));
				Output<Rmap> out = conn.output(dst);) {
			while (count.get() < limit)
				if (null != ex) try {
					ex.submit(() -> stress1(out));
				} catch (RejectedExecutionException e) {
					logger.error(
							() -> "Thread pool overflow, incease [dataggr.migrate.kafka.stress.paral] larger than default [10] or set to 0 for infinite.");
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e1) {}
				}
				else stress1(out);
		} finally {
			if (null != ex) ex.shutdown();
		}
	}

	private static ExecutorService ex() {
		ExecutorService ex;
		if (paral == 0) ex = Executors.newCachedThreadPool();
		else if (paral == 1) ex = null;
		else if (paral > 0) ex = Executors.newFixedThreadPool(paral);
		else if (paral == -1) ex = ForkJoinPool.commonPool();
		else ex = new ForkJoinPool(-paral);
		logger.info("ThreadPool: " + ex);
		return ex;
	}

	private static void stress1(Output<Rmap> out) {
		long now = System.currentTimeMillis();
		try {
			out.enqueue(build(schema));
		} finally {
			long t = spent.addAndGet(System.currentTimeMillis() - now) / 1000;
			long c = count.incrementAndGet();
			if (t > 1 && c % 20000 == 0) logger.warn(() -> "Avro stress sent [" + c + " recs], spent [" + t + " s], avg [" + c / t
					+ " rec/sec]." + (null != ex ? "\n\tpool: " + ex.toString() : ""));;
		}
	}

	private static Schema load() {
		String cp = "/";// + StressTest.class.getPackageName().replaceAll("\\.", "/") + "/";
		String json = String.join("\n", IOs.readLines(StressTest.class.getResourceAsStream(cp + "hik-schema.json")));
		return new Schema.Parser().parse(json);
	}

	private static Sdream<Rmap> build(Schema schema) {
		Rmap r = new Rmap(KafkaDemo.TOPIC);
		schema.getFields().forEach(f -> r.put(f.name(), natual(AvroFormat.Builder.restrictType(f))));
		return Sdream.of1(r);
	}

	private static Random RDM = new Random();

	private static Object natual(Schema.Type t) {
		switch (t) {
		case STRING:
			return UUID.randomUUID().toString();
		case BYTES:
			byte[] bs = new byte[1];
			RDM.nextBytes(bs);
			return bs[0];
		case INT:
			return RDM.nextInt(1000);
		case LONG:
			return RDM.nextLong();
		case FLOAT:
			return RDM.nextFloat();
		case DOUBLE:
			return RDM.nextDouble();
		case BOOLEAN:
			return RDM.nextBoolean();
		default:
			throw new IllegalArgumentException(t.toString());
		}
	}
}
