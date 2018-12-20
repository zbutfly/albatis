import java.util.List;

import com.hzcominfo.albatis.nosql.Connection;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Rmap;

public class kin {
	public static void main(String... args) throws Exception {
		List<String> argus = Colls.list(args);
		URISpec u = new URISpec(argus.remove(0));
		try (Connection c = Connection.connect(u); Input<Rmap> in = c.input(argus.toArray(new String[0]));) {
			in.open();
			while (true) {
				in.dequeue(s -> s.eachs(m -> System.out.println(m)));
				System.err.println("Waiting 1 second...");
				Thread.sleep(1000);
			}
		} finally {
			System.err.println("Finished!");
		}
	}
}
