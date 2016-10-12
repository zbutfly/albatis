package net.butfly.albatis.hbase;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import net.butfly.albacore.utils.IOs;
import net.butfly.albacore.utils.Utils;
import net.butfly.albacore.utils.logger.Logger;

public final class Hbases extends Utils {
	private static final Logger logger = Logger.getLogger(Hbases.class);

	public static Connection connect() throws IOException {
		Configuration hconf = HBaseConfiguration.create();
		Properties conf = IOs.loadAsProps("hbase.properties");
		Set<String> keys = conf.stringPropertyNames();
		for (Field f : HConstants.class.getFields()) {
			int mod = f.getModifiers();
			if (Modifier.isFinal(mod) && Modifier.isStatic(mod) && f.getType().equals(String.class)) {
				String confName;
				try {
					confName = (String) f.get(null);
				} catch (IllegalArgumentException | IllegalAccessException e) {
					continue;
				}
				if (keys.contains(confName)) hconf.set(confName, conf.getProperty(confName));
			}
		}
		return ConnectionFactory.createConnection(hconf);
	}
}
