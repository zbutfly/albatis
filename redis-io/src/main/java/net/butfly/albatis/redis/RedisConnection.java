package net.butfly.albatis.redis;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import com.hzcominfo.albatis.nosql.NoSqlConnection;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.serder.JsonSerder;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;
import redis.clients.jedis.Jedis;

public class RedisConnection extends NoSqlConnection<Jedis> {
	final static String schema = "redis";

	private final Jedis jedis;
	public RedisConnection(URISpec uriSpec) {
		InetSocketAddress[] isas = uriSpec.getInetAddrs();
		if (isas.length <= 0) throw new RuntimeException("Redis uriSpec is empty!");
		jedis = new Jedis(isas[0].getHostString(), isas[1].getPort());
		jedis.auth(uriSpec.getUsername());
	}
	
	@Override
	public <M extends Rmap> Input<M> input(TableDesc... table) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <M extends Rmap> Output<M> output(TableDesc... table) throws IOException {
		// TODO Auto-generated method stub
		return null;
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
			return Colls.list("redis");
		}
	}
	
	public static void main(String[] args) {
		Jedis jedis = new Jedis("172.16.17.22", 6379);
		jedis.auth("b840fc02d524045429941cc15f59e41cb7be6c52");
		List<String> result = jedis.lrange("AFTER_VEHICLE", 0, 1);
		List<HashMap<String, Object>> collect = result.stream().map(r -> JsonSerder.JSON_MAPPER.der(r, HashMap.class)).collect(Collectors.toList());
		System.out.println(collect.toString());
//		jedis.lpush("AFTER_VEHICLE", "{\"jkxlh\":\"999999\",\"kkid\":\"3306820038\",\"sbbh\":\"3306820038\",\"hphm\":\"浙DT7351\",\"hpzl\":\"02\",\"hpys\":\"2\",\"cllx\":\"K\",\"gwsj\":\"2018-10-12 11:32:53\",\"fxbh\":\"02\",\"cdbh\":\"5\",\"clsd\":\"0\",\"jllx\":\"\",\"sjly\":\"1\",\"sbcj\":\"2\",\"clpp\":\"0\",\"csys\":\"Z\",\"cpzb\":\"000,000,000,000\",\"jsszb\":\"000,000,000,000\",\"fjsszb\":\"000,000,000,000\",\"tplx\":\"1\",\"tztp\":\"\",\"tp1\":\"http://33.170.2.134:6120/pic?2dd866z27-=s8a7181103a10--1c84551945003i3b3*=5d9*60d6i*s1d=i2p5t=pe*m0i21=661b02686-e88i32f*e27ci85=\",\"tp4\":\"http://33.170.2.134:6120/pic?=d4ii3fe*8da3886e-662a0b-62c84551945003i3b3*=6d9*61d1i*s1d=i7p3t=pe*m0i21=-110130-11875b8s=d23dz\",\"receivedTime\":1539315179216,\"version\":\"1.0.0\",\"fromIP\":\"33.170.215.12\"}");
//		String r = jedis.lpop("AFTER_VEHICLE");
//		String r1 = jedis.lpop("AFTER_VEHICLE");
//		System.out.println(r);
//		System.out.println(r1);
		jedis.close();
	}
}
