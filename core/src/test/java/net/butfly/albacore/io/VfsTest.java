package net.butfly.albacore.io;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.Texts;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.Connection;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.vfs.VfsConnection;

public class VfsTest {
	private static final String table = "test-data";
	// private static final String uri = "file:///C:/Workspaces/alfames/albatis/core/vfs-test/default-data.json";
	private static final String uri = "ftp://127.0.0.1/test2/default-data.json";
	private static final Random random = new Random();

	public static void main(String[] args) throws IOException {
		try (VfsConnection vfs = Connection.connect(new URISpec(uri)); Output<Rmap> o = vfs.output();) {
			for (long i = 0; i < 100; i++)
				o.enqueue(items(100, i));
		}
	}

	private static Sdream<Rmap> items(int c, long index) {
		List<Rmap> l = Colls.list();
		for (int i = 0; i < c; i++) {
			Rmap r = new Rmap(table);
			r.put("id", UUID.randomUUID().toString());
			r.put("name", Texts.randomGBK());
			r.put("age", random.nextInt(100));
			r.put("round", index);
			r.put("index", i);
			l.add(r);
		}
		return Sdream.of(l);
	}
}
