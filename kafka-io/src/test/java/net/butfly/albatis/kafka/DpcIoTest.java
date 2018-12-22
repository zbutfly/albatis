package net.butfly.albatis.kafka;

import java.util.List;
import java.util.UUID;

import com.hzcominfo.albatis.in;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Colls;

public class DpcIoTest {
	public static void main(String... args) throws Exception {
		// // bson as default and output an map toString
		// input("kafka://data01:2181,data02:2181,data03:2181/kafka", "flow_PEOPLE_1218_DBRW_inc");
		// bson as default and output as json
		// input("kafka://data01:2181,data02:2181,data03:2181/kafka?df=bson,rev:json", "flow_PEOPLE_1218_DBRW_inc");
		// string kafka without key (null key)
		input("kafka://data01:2181,data02:2181,data03:2181/kafka?df=json", "KAFKA_STRING_TEST");
//		input("kafka://data01:2181,data02:2181,data03:2181/kafka?df=jsons", "HIK_STR_TEST");
	}

	public static void input(String uri, String... topics) throws Exception {
		List<String> l = Colls.list(topics);
		l.add(0, new URISpec(uri).reauth("KafkaInputTest-" + UUID.randomUUID()).toString());
		in.main(l.toArray(new String[0]));
	}
}
