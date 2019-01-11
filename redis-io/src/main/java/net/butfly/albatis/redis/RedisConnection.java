package net.butfly.albatis.redis;

import java.io.IOException;
import java.util.List;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.Utf8StringCodec;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.DataConnection;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.IOFactory;
import net.butfly.alserdes.SerDes;

@SuppressWarnings("unchecked")
@SerDes.As("json")
public class RedisConnection<T> extends DataConnection<RedisClient> implements IOFactory {
	final static String schema = "redis";
	private final RedisCodec<T, T> codec;
	public final StatefulRedisConnection<T, T> redis;
	public final RedisCommands<T, T> sync;
	final RedisAsyncCommands<T, T> async;
	private final Function<String, T> keying;

	public RedisConnection(URISpec uriSpec) throws IOException {
		super(uriSpec, "redis");
		Class<?> nativeClass = formats().get(0).formatClass();
		if (String.class.isAssignableFrom(nativeClass)) {
			codec = (RedisCodec<T, T>) new Utf8StringCodec();
			keying = s -> (T) s;
		} else if (byte[].class.isAssignableFrom(nativeClass)) {
			codec = (RedisCodec<T, T>) new ByteArrayCodec();
			keying = s -> (T) ((String) s).getBytes();
		} else throw new IllegalArgumentException();
		redis = client.connect(codec);
		sync = redis.sync();
		async = redis.async();
	}

	public T keying(String s) {
		return null == s ? null : keying.apply(s);
	}

	@Override
	protected RedisClient initialize(URISpec uri) {
		return RedisClient.create(RedisURI.create(uri.toURI()));
	}

	@Override
	public void close() {
		try {
			super.close();
		} catch (IOException e) {}
		redis.close();
		client.shutdown();
	}

	@Override
	public RedisListInput<T> inputRaw(TableDesc... table) throws IOException {
		return new RedisListInput<>(this, table);
	}

	@Override
	public RedisOutput<T> outputRaw(TableDesc... table) throws IOException {
		return new RedisOutput<>(this, table);
	}

	@SuppressWarnings("rawtypes")
	public static class Driver implements net.butfly.albatis.Connection.Driver<RedisConnection> {
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
}
