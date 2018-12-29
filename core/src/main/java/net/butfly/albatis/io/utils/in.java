package net.butfly.albatis.io.utils;

import java.util.List;
import java.util.function.Consumer;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.Connection;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Rmap;

public class in {
	public static void main(String... args) {
		List<String> argus = Colls.list(args);
		input(new URISpec(argus.remove(0)), argus, System.out::println);
	}

	public static void input(URISpec u, List<String> tables, Consumer<Rmap> using) {
		try (Connection c = Connection.connect(u); Input<Rmap> in = c.input(tables.toArray(new String[0]));) {
			in.open();
			// int sleep = 0;
			while (in.opened()) {
				in.dequeue(s -> s.eachs(using::accept));
				// System.err.println("Waiting " + ++sleep + " second...");
				Thread.sleep(1000);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			System.err.println("Finished!");
		}
	}
}
