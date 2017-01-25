package net.butfly.albatis.hbase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.IOs;
import net.butfly.albacore.utils.Utils;
import net.butfly.albacore.utils.logger.Logger;

public final class Hbases extends Utils {
	protected static final Logger logger = Logger.getLogger(Hbases.class);

	public static Connection connect() throws IOException {
		return connect(null);
	}

	public static Connection connect(Properties conf) throws IOException {
		Configuration hconf = HBaseConfiguration.create();
		if (null != conf && !conf.isEmpty()) {
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
		}
		return ConnectionFactory.createConnection(hconf);
	}

	public static String colFamily(Cell cell) {
		return Bytes.toString(CellUtil.cloneFamily(cell)) + ":" + Bytes.toString(CellUtil.cloneQualifier(cell));
	}

	public static <T> Map<String, T> mapCols(HbaseResult r, Converter<byte[], T> conv) {
		Map<String, T> v = new HashMap<>();
		r.each(c -> {
			try {
				v.put(Bytes.toString(CellUtil.cloneQualifier(c)), conv.apply(CellUtil.cloneValue(c)));
			} catch (Exception e) {
				logger.error("Hbase read failure", e);
			}
		});
		return v;

	}

	public static byte[] toBytes(Result result) throws IOException {
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream();) {
			for (Cell c : result.rawCells())
				IOs.writeBytes(baos, cellToBytes(c));
			return baos.toByteArray();
		}
	}

	public static Result toResult(byte[] bytes) throws IOException {
		List<Cell> cells = new ArrayList<>();
		try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes)) {
			byte[] buf;
			while ((buf = IOs.readBytes(bais)) != null && buf.length > 0)
				cells.add(cellFromBytes(buf));
			return Result.create(cells);
		}
	}

	public static byte[] cellToBytes(Cell cell) throws IOException {
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream();) {
			IOs.writeBytes(baos, CellUtil.cloneRow(cell));
			IOs.writeBytes(baos, CellUtil.cloneFamily(cell));
			IOs.writeBytes(baos, CellUtil.cloneQualifier(cell));
			IOs.writeBytes(baos, CellUtil.cloneValue(cell));
			return baos.toByteArray();
		}
	}

	public static Cell cellFromBytes(byte[] bytes) throws IOException {
		try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes)) {
			return new KeyValue(IOs.readBytes(bais), IOs.readBytes(bais), IOs.readBytes(bais), IOs.readBytes(bais));
		}
	}

}
