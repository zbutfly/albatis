package net.butfly.albatis.mongodb;

import java.io.Closeable;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.ServerAddress;

import net.butfly.albacore.utils.IOs;
import net.butfly.albacore.utils.Instances;
import net.butfly.albacore.utils.Utils;
import net.butfly.albacore.utils.async.Concurrents;
import net.butfly.albacore.utils.logger.Logger;

public final class Mongos extends Utils {
	private static final Logger logger = Logger.getLogger(Mongos.class);

	public static final class MDB implements Closeable {
		private DB db;

		public MDB(DB db) {
			super();
			this.db = db;
		}

		public DB db() {
			return db;
		}

		@Override
		public void close() {
			db = null;
		}

	}

	@SuppressWarnings("deprecation")
	public static MDB mongoConnect(String configFile) {
		Properties conf = IOs.loadAsProps(configFile);
		String host = conf.getProperty("mongodb.host", "localhost");
		int port = Integer.parseInt(conf.getProperty("mongodb.port", "30012"));
		List<ServerAddress> sas = new ArrayList<>();
		for (String h : host.split(","))
			try {
				sas.add(new ServerAddress(h, port));
			} catch (UnknownHostException e) {
				throw new RuntimeException(e);
			}
		Mongo mongo = null;
		do {
			mongo = Instances.fetch(() -> new Mongo(sas), Mongo.class, host, port);
		} while (mongo == null && Concurrents.waitSleep(10000, logger, "MongoDB connect failure"));
		do {
			DB db = mongo.getDB(conf.getProperty("mongodb.db"));
			if (null != db && db.authenticate(conf.getProperty("mongodb.username"), conf.getProperty("mongodb.password").toCharArray()))
				return new MDB(db);
		} while (Concurrents.waitSleep(10000));
		return null;
	}
}
