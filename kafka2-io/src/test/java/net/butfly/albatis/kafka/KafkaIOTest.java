package net.butfly.albatis.kafka;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.utils.in;
import net.butfly.albatis.io.utils.out;

public class KafkaIOTest {
	public static void main(String... args) throws Exception {
		URISpec uri = new URISpec("kafka://data01:2181,data02:2181,data03:2181/kafka");
		// bson as default and etl as default
		// input(uri, "flow_PEOPLE_1218_DBRW_inc");

		// bson as default and output as json
		uri.setParameter("df", "bson,rev:json");
		input(uri, "flow_PEOPLE_1218_DBRW_inc");

		// string kafka without key (null key)
		// input("kafka://data01:2181,data02:2181,data03:2181/kafka?df=json", "KAFKA_STRING_TEST");
		// input("kafka://data01:2181,data02:2181,data03:2181/kafka?df=jsons", "HIK_STR_TEST");
	}

	public static void input(URISpec uri, String topic) throws Exception {
		in.main(uri.reauth("KafkaInputTest-" + UUID.randomUUID()).toString(), topic);
	}

	public static void inputOutput(URISpec uri, String srcTopic, String dstTopic) throws Exception {
		BlockingQueue<Rmap> q = new LinkedBlockingQueue<>();
		Thread o = new Thread(() -> out.output(uri, "TEST_ZX_BSON", "id", (t, kf) -> {
			try {
				return q.take();
			} catch (InterruptedException e) {
				return null;
			}
		}));
		o.start();
		in.input(uri.reauth("KafkaInputTest-" + UUID.randomUUID()), Colls.list(srcTopic), q::offer);
	}
}
