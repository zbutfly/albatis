package com.hzcominfo.hiktest;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.hikvision.multidimensional.kafka.register.schema.KafkaDemo;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.Texts;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.Connection;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.kafka.avro.AvroFormat;
import net.butfly.albatis.kafka.avro.SchemaReg;

public class CominfoTest {
	public static void main(String... args) throws IOException {
		System.setProperty(SchemaReg.SCHEMA_REGISTRY_URL_CONF, KafkaDemo.SCHEMA_REG);
		TableDesc dst = AvroFormat.Builder.schema(KafkaDemo.TOPIC, KafkaDemo.USER_SCHEMA);
		try (Connection c = Connection.connect(new URISpec("kafka2:bootstrap://" + KafkaDemo.BOOTSTRAP + "?df=avro"));
				Output<Rmap> o = c.output(dst);) {
			o.enqueue(Sdream.of(data(dst.qualifier.name, 10)));
		}
	}

	private static List<Rmap> data(String table, int c) {
		List<Map<String, Object>> l = Colls.list();
		for (int i = 0; i < c; i++)
			l.add(Maps.of("id", i, "name", "name@" + Texts.iso8601(new Date()) + "##" + i, "age", (int) (Math.random() * 10 + 20)));
		return Colls.list(l, m -> new Rmap(table, m).keyField("id"));
	}
}
