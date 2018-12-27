package net.butfly.albatis.redis;

import java.io.IOException;
import java.util.List;

import com.hzcominfo.albatis.nosql.DataConnection;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.codec.Utf8StringCodec;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.TypelessIO;

public class RedisConnection extends DataConnection<RedisClient> implements TypelessIO {
	final static String schema = "redis";
	public final String type;

	public final RedisClient redisClient;

	public RedisConnection(URISpec uriSpec) throws IOException {
		super(uriSpec, "redis");
		redisClient = RedisClient.create(uriSpec.toString());
		type = uriSpec.getParameter("type");
	}

	@Override // TODO: use serder class, not param type
	public RedisListInput inputRaw(TableDesc... table) throws IOException {
		List<String> l = Colls.list(t -> t.name, table);
		if (type.equals("byteArray")) return new RedisListInput("RedisListInput", this, new ByteArrayCodec(), l.toArray(new Object[0]));
		else if (type.equals("string")) return new RedisListInput("RedisListInput", this, new StringCodec(), l.toArray(new Object[0]));
		return new RedisListInput("RedisListInput", this, new Utf8StringCodec(), l.toArray(new Object[0]));
	}

	@Override
	public RedisOutput outputRaw(TableDesc... table) throws IOException {
		return new RedisOutput("RedisOutput", this);
	}

	public static class Driver implements com.hzcominfo.albatis.nosql.Connection.Driver<RedisConnection> {
		static {
			DriverManager.register(new Driver());
		}

		@Override
		public RedisConnection connect(URISpec uriSpec) throws IOException {
			return new RedisConnection(uriSpec);
		}

		@Override
		public List<String> schemas() {
			return Colls.list("redis", "redis:list");
		}
	}

	public static void main(String[] args) {
		URISpec u = new URISpec("redis://b840fc02d524045429941cc15f59e41cb7be6c52@172.16.17.22:6379");
		System.out.println(u.toString());
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
}
