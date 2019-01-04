package net.butfly.albatis.io.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.Connection;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;
import net.butfly.alserdes.json.JsonSerDes;

public class out {
	private static final JsonSerDes json = new JsonSerDes();
//	private static final Format json = Format.of("json");

	public static void main(String... args) {
		List<String> argus = Colls.list(args);
		URISpec uri = new URISpec(argus.remove(0));
		BufferedReader lr = new BufferedReader(new InputStreamReader(System.in));
		output(uri, argus.remove(0), argus.isEmpty() ? null : argus.remove(0), (tableName, keyField) -> {
			String l;
			while (true) {
				try {
					l = lr.readLine();
				} catch (IOException e) {
					return null;
				}
				if (null == l) return null;
				if (l.matches("^\\s*//.*")) continue;
				Map<String, Object> m = json.deser(l);
				if (null == m) System.err.print("Wrong line: " + l);
				else {
					Rmap rm = new Rmap(tableName, m);
					if (null != keyField) rm = rm.keyField(keyField);
					else rm.key(UUID.randomUUID());
					return rm;
				}
			}
		});
	}

	public static void output(URISpec uri, String table, String keyField, BiFunction<String, String, Rmap> getting) {
		Rmap r = null;
		int count = 0;
		try (Connection c = Connection.connect(uri); Output<Rmap> out = c.output();) {
			out.open();
			while (null != (r = getting.apply(table, keyField))) {
				out.enqueue(Sdream.of1(r));
				System.err.print("Writen Rmaps: " + ++count);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			System.err.println("Finished!");
		}
	}
}
