package com.hzcominfo.hiktest;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.Schema;

import com.hikvision.multidimensional.kafka.register.schema.KafkaDemo;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.IOs;
import net.butfly.albatis.Connection;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.kafka.avro.AvroFormat;
import net.butfly.albatis.kafka.avro.SchemaReg;

public class StressTest {
	private static AtomicLong count = new AtomicLong(), spent = new AtomicLong();
	private static Schema schema = load();
	private static int paral = Integer.parseInt(System.getProperty("dataggr.migrate.kafka.stress.paral", "10"));

	public static void main(String[] args) throws IOException {
		System.setProperty(SchemaReg.SCHEMA_REGISTRY_URL_CONF, KafkaDemo.SCHEMA_REG);
		TableDesc dst = AvroFormat.Builder.schema(KafkaDemo.TOPIC, schema);

		ExecutorService ex = paral > 0 ? Executors.newFixedThreadPool(paral) : Executors.newCachedThreadPool();

		try (Connection conn = Connection.connect(new URISpec("kafka2:bootstrap://" + KafkaDemo.BOOTSTRAP + "?df=avro"));
				Output<Rmap> out = conn.output(dst);) {
			try {
				ex.submit(() -> stress1(out));
			} catch (RejectedExecutionException e) {
				System.err.println(
						"Thread pool overflow, incease [dataggr.migrate.kafka.stress.paral] larger than default [10] or set to 0 for infinite.");
			}
		} finally {
			ex.shutdown();
		}
	}

	private static void stress1(Output<Rmap> out) {
		long now = System.currentTimeMillis();
		try {
			out.enqueue(build(schema));
		} finally {
			long t = spent.addAndGet(System.currentTimeMillis() - now);
			long c = count.incrementAndGet();
			if (c % 1000 == 0) System.err.println("Avro stress sent [" + c + " recs], spent [" + t + " ms].");
		}
	}

	private static Schema load() {
		String cp = "/" + StressTest.class.getPackageName().replaceAll("\\.", "/") + "/";
		String json = String.join("\n", IOs.readLines(StressTest.class.getResourceAsStream(cp + "hik-schema.json")));
		return new Schema.Parser().parse(json);
	}

	private static Sdream<Rmap> build(Schema schema) {
		Rmap r = new Rmap(KafkaDemo.TOPIC);
		for (Schema.Field f : schema.getFields())
			r.put(f.name(), natual(AvroFormat.Builder.restrictType(f)));
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
