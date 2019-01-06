package net.butfly.albatis.redis;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.serder.JsonSerder;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.Connection;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;

public class RedisTest {
	public static void main(String[] args) throws IOException {
		URISpec u = new URISpec("redis://b840fc02d524045429941cc15f59e41cb7be6c52@172.16.17.22:6379");
		System.out.println(u.toString());
		try (RedisConnection<String> c = Connection.connect(u);) {
			get2(c);
			sl();
		}
	}

	public static void write0(RedisConnection<String> c) throws IOException {
		try (Output<Rmap> out = c.output();) {
			for (int i = 0; i < 10; i++)
				Exeter.of().submit(() -> {
					out.enqueue(random((int) (Math.random() * 10 + 1)));
				});
		}
	}

	public static void write(RedisConnection<String> c) throws IOException {
		try (Output<Rmap> out = c.output();) {
			for (int i = 0; i < 10; i++)
				Exeter.of().submit(() -> {
					while (true)
						out.enqueue(random((int) (Math.random() * 10 + 1)));
				});
		}
	}

	// overflow!
	public static void get2(RedisConnection<String> c) throws IOException {
		for (int i = 0; i < 10; i++)
			Exeter.of().submit(() -> {
				while (true)
					for (String k : keys)
						try (StatefulRedisConnection<String, String> r = c.client.connect()) {
							String v = r.sync().get(k);
							System.err.println(k + " => " + v);
							r.async().set(k, v);
						}
			});
	}

	public static void get1(RedisConnection<String> c) throws IOException {
		for (int i = 0; i < 10; i++)
			Exeter.of().submit(() -> {
				while (true)
					for (String k : keys) {
						String v = c.sync.get(k);
						System.err.println(k + " => " + v);
						c.async.set(k, v);
					}
			});
	}

	public static void get(RedisConnection<String> c) throws IOException {
		for (int i = 0; i < 10; i++)
			Exeter.of().submit(() -> {
				while (true)
					for (String k : keys)
						System.err.println(c.sync.get(k));
			});
	}

	private static final String[] keys = new String[] { "DPC:CODEMAP:AFTER_VEHICLE:f6c065df-d1f2-416c-8226-53ed19de4400",
			"DPC:CODEMAP:AFTER_VEHICLE:a766b2cd-5fa2-4ee3-96f9-935cc17d645b",
			"DPC:CODEMAP:AFTER_VEHICLE:f960e051-af4a-41e0-a647-91f2c2dc0996",
			"DPC:CODEMAP:AFTER_VEHICLE:4320a36f-49e8-4fe8-9a97-8719c43e2bd6",
			"DPC:CODEMAP:AFTER_VEHICLE:a7f21400-1b3e-4316-a0df-bcbbd4ddd644",
			"DPC:CODEMAP:AFTER_VEHICLE:2dbbf621-93fe-448b-8295-a7999bc37a32",
			"DPC:CODEMAP:AFTER_VEHICLE:31374c6b-3981-46ad-95fd-05d91db0b633",
			"DPC:CODEMAP:AFTER_VEHICLE:ee010246-1843-45f2-926d-460085c24ecf",
			"DPC:CODEMAP:AFTER_VEHICLE:59e7f50f-0a1a-444d-ba59-3b0ab3613ab3",
			"DPC:CODEMAP:AFTER_VEHICLE:19cbb347-71c2-4676-b787-5c5d36eb8d95",
			"DPC:CODEMAP:AFTER_VEHICLE:3cad2f94-2eaf-4077-9177-dd453a2529ca",
			"DPC:CODEMAP:AFTER_VEHICLE:42cae7e9-bffe-4d3e-84dd-ad82979eb52d",
			"DPC:CODEMAP:AFTER_VEHICLE:2b797afa-6cb8-40c6-b641-8675dff0f2c6",
			"DPC:CODEMAP:AFTER_VEHICLE:10092929-fb16-4335-b61b-48571b9f58ff",
			"DPC:CODEMAP:AFTER_VEHICLE:8d1a0d54-ffa2-45d6-ac34-75bc62034fdc",
			"DPC:CODEMAP:AFTER_VEHICLE:8d30d26f-d962-45c5-aad3-c406a989b014",
			"DPC:CODEMAP:AFTER_VEHICLE:82e16ee2-efa6-40ee-8833-013767ac0915",
			"DPC:CODEMAP:AFTER_VEHICLE:50528728-3e74-43b7-832e-73d0f78cfa28",
			"DPC:CODEMAP:AFTER_VEHICLE:89fd9896-e3fc-4a60-bf64-93c6eb8b0b50",
			"DPC:CODEMAP:AFTER_VEHICLE:c2a1dc98-9987-4a0a-9b80-c81e28c3f7f8",
			"DPC:CODEMAP:AFTER_VEHICLE:94f97590-8567-45e5-b5b7-df31be23c63c",
			"DPC:CODEMAP:AFTER_VEHICLE:8654237c-36fc-4428-adbe-307736335588",
			"DPC:CODEMAP:AFTER_VEHICLE:b4555fc1-5b9f-48fc-873e-11667b07e92f",
			"DPC:CODEMAP:AFTER_VEHICLE:76225bfe-5047-449c-883e-9b82e0d542c9",
			"DPC:CODEMAP:AFTER_VEHICLE:9756b9f2-5200-4a78-bfb7-0aa7aa728df4",
			"DPC:CODEMAP:AFTER_VEHICLE:4d581a5f-026a-4e64-be91-ecf551982bd8",
			"DPC:CODEMAP:AFTER_VEHICLE:a5b38902-7b96-4a54-9118-e54e12c372f9",
			"DPC:CODEMAP:AFTER_VEHICLE:d66d1a44-9bed-48a3-a617-7a233acd2d22",
			"DPC:CODEMAP:AFTER_VEHICLE:b5cba46e-ed5d-4e67-8ce7-9a74d2621629",
			"DPC:CODEMAP:AFTER_VEHICLE:17aac212-73c6-4ab7-8549-e388d86a07bc",
			"DPC:CODEMAP:AFTER_VEHICLE:22917dd9-cfab-4946-ae90-7536bcf8d60a",
			"DPC:CODEMAP:AFTER_VEHICLE:03398612-63de-4605-859b-8e41f982ef48",
			"DPC:CODEMAP:AFTER_VEHICLE:8dcea028-84a2-415d-8012-206996579373",
			"DPC:CODEMAP:AFTER_VEHICLE:dd2ffec3-4278-4733-8d93-b96a8c8c6a54",
			"DPC:CODEMAP:AFTER_VEHICLE:5863deba-2ef7-4513-b09a-8bfea8601506",
			"DPC:CODEMAP:AFTER_VEHICLE:78c57a1a-01cc-47a3-b249-5f455e244a7c",
			"DPC:CODEMAP:AFTER_VEHICLE:126b5de4-b90d-4f22-b666-0f3cd297cb40",
			"DPC:CODEMAP:AFTER_VEHICLE:c56c510f-69b9-4dc5-8c01-95a6809305e6",
			"DPC:CODEMAP:AFTER_VEHICLE:12189955-d8dc-4bcf-9d28-84eb55fb8713",
			"DPC:CODEMAP:AFTER_VEHICLE:d6896154-55ca-47bb-9c30-c408375edc44",
			"DPC:CODEMAP:AFTER_VEHICLE:59a187b7-9f14-42ca-ac66-5c7535967341",
			"DPC:CODEMAP:AFTER_VEHICLE:f7db1006-5728-4a91-9e0a-622b90face58",
			"DPC:CODEMAP:AFTER_VEHICLE:0c76f597-ca2b-4bb3-b557-e3de097488f8",
			"DPC:CODEMAP:AFTER_VEHICLE:0ebaaffd-62a4-4942-8f06-dc58b4f9d20a",
			"DPC:CODEMAP:AFTER_VEHICLE:b9ab67a6-064a-4dc2-be66-a80de86e385c",
			"DPC:CODEMAP:AFTER_VEHICLE:09c16c85-6d26-42a0-a432-e1976dffa4c0",
			"DPC:CODEMAP:AFTER_VEHICLE:870603e2-5a6c-402c-91e7-451dcc866525",
			"DPC:CODEMAP:AFTER_VEHICLE:741df3b6-a808-4126-8c43-bcbdd4d5053b",
			"DPC:CODEMAP:AFTER_VEHICLE:6a303195-696e-4183-b9d5-5f61662de1dc",
			"DPC:CODEMAP:AFTER_VEHICLE:8955ea43-a924-41e1-8580-5e455c329487",
			"DPC:CODEMAP:AFTER_VEHICLE:869e4da2-0aa7-4cf9-9ed9-7e1c5e43295d",
			"DPC:CODEMAP:AFTER_VEHICLE:30a3f6d2-29a3-4024-8718-ff1142eb78b8",
			"DPC:CODEMAP:AFTER_VEHICLE:5a4288d3-c769-462f-9a2e-7b8ac153673f",
			"DPC:CODEMAP:AFTER_VEHICLE:71c5c8c2-7e3d-489a-b249-ee18f245cb6c" };

	private static Sdream<Rmap> random(int c) {
		List<Rmap> l = Colls.list();
		for (int i = 0; i < c; i++)
			l.add(new Rmap("AFTER_VEHICLE", null, UUID.randomUUID().toString(), //
					JsonSerder.JSON_MAPPER.ser(Maps.of("RANDOM", UUID.randomUUID().toString()))));
		return Sdream.of(l);
	}

	public static void test(URISpec u) {
		RedisClient redisClient = RedisClient.create(u.toString());
		StatefulRedisConnection<String, String> connection = redisClient.connect();
		RedisCommands<String, String> syncCommands = connection.sync();
		syncCommands.lpush("AFTER_VEHICLE",
				"{\"jkxlh\":\"999999\",\"kkid\":\"3306820038\",\"sbbh\":\"3306820038\",\"hphm\":\"浙DT7351\",\"hpzl\":\"02\",\"hpys\":\"2\",\"cllx\":\"K\",\"gwsj\":\"2018-10-12 11:32:53\",\"fxbh\":\"02\",\"cdbh\":\"5\",\"clsd\":\"0\",\"jllx\":\"\",\"sjly\":\"1\",\"sbcj\":\"2\",\"clpp\":\"0\",\"csys\":\"Z\",\"cpzb\":\"000,000,000,000\",\"jsszb\":\"000,000,000,000\",\"fjsszb\":\"000,000,000,000\",\"tplx\":\"1\",\"tztp\":\"\",\"tp1\":\"http://33.170.2.134:6120/pic?2dd866z27-=s8a7181103a10--1c84551945003i3b3*=5d9*60d6i*s1d=i2p5t=pe*m0i21=661b02686-e88i32f*e27ci85=\",\"tp4\":\"http://33.170.2.134:6120/pic?=d4ii3fe*8da3886e-662a0b-62c84551945003i3b3*=6d9*61d1i*s1d=i7p3t=pe*m0i21=-110130-11875b8s=d23dz\",\"receivedTime\":1539315179216,\"version\":\"1.0.0\",\"fromIP\":\"33.170.215.12\"}");

		// while (true) {
		// Object r = syncCommands.lpop("AFTER_VEHICLE");
		// System.out.println(r);
		// }
		// connection.close();
		// redisClient.shutdown();
	}

	private static void sl() {
		while (true)
			try {
				Thread.sleep(Long.MAX_VALUE);
			} catch (InterruptedException e) {}

	}
}
