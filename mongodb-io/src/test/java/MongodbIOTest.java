import java.io.IOException;

import com.mongodb.DBObject;

import net.butfly.albacore.io.impl.ConsoleOutput;
import net.butfly.albatis.mongodb.MongodbInput;

public class MongodbIOTest {
	public static void main(String[] args) throws IOException {
		try (ConsoleOutput<DBObject> out = new ConsoleOutput<>(dbo -> dbo.toMap().toString());
				MongodbInput in = new MongodbInput("CZRK_TEST", "mongodb.properties", 1, 1000, false);) {
			in.pump(out, 100, 10).start().waiting();
		}
	}
}
