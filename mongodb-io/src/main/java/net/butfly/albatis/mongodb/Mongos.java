package net.butfly.albatis.mongodb;

import java.io.Closeable;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.ServerAddress;

import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.Instances;
import net.butfly.albacore.utils.Utils;
import net.butfly.albacore.utils.async.Concurrents;
import net.butfly.albacore.utils.logger.Logger;

@Deprecated
public final class Mongos extends Utils {
	private static final Logger logger = Logger.getLogger(Mongos.class);

	public static final class MongoDB extends MongodbConnection {
		public MongoDB(String uri) throws IOException {
			super(uri);
		}

		public MongoDB(String host, int port, String dbname, String username, String password) throws IOException {
			super("mongodb://" + username + ":" + password + "@" + host + ":" + port + "/" + dbname);
		}

		public DB connect() {
			return super.db();
		}

		public DB connect(String dbname) {
			return super.db(dbname);
		}

		@Override
		public void close() throws IOException {
			super.close();
		}
	}

	public static final class MConnection implements Closeable {
		private DB db;

		public MConnection(DB db) {
			super();
			this.db = db;
		}

		public DB connection() {
			return db;
		}

		@Override
		public void close() {
			db = null;
		}
	}

	public static MConnection connect(String configFile) {
		Properties conf;
		try {
			conf = Configs.read(configFile);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		String host = conf.getProperty("mongodb.host", "localhost");
		int port = Integer.parseInt(conf.getProperty("mongodb.port", "30012"));
		List<ServerAddress> sas = new ArrayList<>();
		for (String h : host.split(","))
			try {
				sas.add(new ServerAddress(h, port));
			} catch (UnknownHostException e) {
				logger.error("MongoDB connect failure", e);
			}
		Mongo mongo = null;
		do {
			mongo = Instances.fetch(() -> new Mongo(sas), Mongo.class, host, port);
		} while (mongo == null && Concurrents.waitSleep(10000, logger, "MongoDB connect failure"));
		do {
			DB db = mongo.getDB(conf.getProperty("mongodb.db"));
			if (null != db && db.authenticate(conf.getProperty("mongodb.username"), conf.getProperty("mongodb.password").toCharArray()))
				return new MConnection(db);
		} while (Concurrents.waitSleep(10000));
		return null;
	}
}
