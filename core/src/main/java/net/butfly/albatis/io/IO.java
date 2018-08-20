package net.butfly.albatis.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.hzcominfo.albatis.nosql.Connection;

import net.butfly.albacore.base.Sizable;
import net.butfly.albacore.io.Openable;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.CaseFormat;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Statistic;
import net.butfly.albatis.ddl.TableDesc;

public interface IO extends Sizable, Openable, Serializable {
	static final Map<IO, Map<String, TableDesc>> SCHEMAS = Maps.of();

	default Map<String, TableDesc> schemaAll() {
		return SCHEMAS.computeIfAbsent(this, io -> Maps.of());
	}

	default TableDesc schema(String table) {
		return schemaAll().getOrDefault(table, TableDesc.dummy(table));
	}

	@SuppressWarnings("unchecked")
	default <T extends IO> T schema(TableDesc... table) {
		logger().info("Schema registered: \n\t" + String.join("\n\t", Colls.list(t -> t.toString(), table)));
		for (TableDesc td : table)
			schemaAll().put(td.name, td);
		return (T) this;
	}

	enum SchemaMode {
		NONE, SINGLE, MULTIPLE
	}

	default SchemaMode schemaMode() {
		Map<String, TableDesc> a = schemaAll();
		switch (schemaAll().size()) {
		case 0:
			return SchemaMode.NONE;
		case 1:
			return SchemaMode.SINGLE;
		default:
			return SchemaMode.MULTIPLE;
		}
	}

	default boolean schemaExists(String table) {
		return schemaAll().containsKey(table);
	}

	// ================
	default Connection connect() throws IOException {
		return Connection.DUMMY;
	}

	default URISpec target() {
		return null;
	}

	@Override
	default void open() {
		stating();
		Openable.super.open();
	}

	default int batchSize() {
		IO b = Wrapper.bases(this);
		return Props.PROPS.computeIfAbsent(this, io -> Maps.of()).computeIfAbsent(Props.BATCH_SIZE, //
				k -> Props.propI(b.getClass(), k, 500)).intValue();
	}

	default int features() {
		return 0;
	}

	interface Feature {
		static final int STREAMING = 0x1;
		static final int WRAPPED = 0x02;
		static final int ODD = 0x04;
		static final int SPARK = 0x08;
	}

	default boolean hasFeature(int... f) {
		int f0 = features();
		for (int ff : f)
			f0 &= ff;
		return 0 != f0;
	}

	default String ser() {
		try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); ObjectOutputStream oos = new ObjectOutputStream(bos);) {
			oos.writeObject(this);
			return Base64.getEncoder().encodeToString(bos.toByteArray());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@SuppressWarnings("unchecked")
	static <T extends IO> T der(String ser) {
		byte[] b = Base64.getDecoder().decode(ser);
		try (ByteArrayInputStream bos = new ByteArrayInputStream(b); ObjectInputStream oos = new ObjectInputStream(bos);) {
			return (T) oos.readObject();
		} catch (IOException | ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	// about stats....
	class Props {
		static final String BATCH_SIZE = "batch.size";
		static final String STATS_STEP = "stats.step";
		static Map<IO, Map<String, Number>> PROPS = Maps.of();

		public static String propName(Class<?> io, String suffix) {
			return "albatis." + CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_DOT, io.getSimpleName()) + "." + suffix;
		}

		private static String propDefName(Class<?> io, String suffix) {
			String name = CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_DOT, io.getSimpleName());
			if (name.endsWith(".input")) name = "*.input";
			else if (name.endsWith(".output")) name = "*.output";
			else if (name.endsWith(".queue")) name = "*.queue";
			else name = "*";
			return "albatis." + name + "." + suffix;
		}

		public static String prop(Class<?> io, String suffix, String def) {
			return Configs.gets(propName(io, suffix), Configs.gets(propDefName(io, suffix), def));
		}

		public static long propL(Class<?> io, String suffix, long def) {
			return Long.parseLong(prop(io, suffix, Long.toString(def)));
		}

		public static int propI(Class<?> io, String suffix, int def) {
			return Integer.parseInt(prop(io, suffix, Integer.toString(def)));
		}

		public static boolean propB(Class<?> io, String suffix, boolean def) {
			return Boolean.parseBoolean(prop(io, suffix, Boolean.toString(def)));
		}
	}

	/**
	 * default disable stats, if inherited and return a valid {@code Statistic}, enable statis on this io instnce.
	 */
	default Statistic trace() {
		return new Statistic(this);
	}

	default void stating() {
		Map<String, Number> props = Props.PROPS.computeIfAbsent(this, io -> Maps.of());
		IO b = Wrapper.bases(this);
		long step = props.computeIfAbsent(Props.STATS_STEP, k -> Props.propL(b.getClass(), k, -1)).longValue();
		logger().debug(() -> "Step set to [" + step + "] by [" + Props.propName(b.getClass(), Props.STATS_STEP) + "]");
		s().step(step);
	}

	default <E> Sdream<E> stats(Sdream<E> s) {
		return s.peek(e -> s().stats(e));
	}

	class Stats {
		private static final Map<IO, Statistic> IO_STATS = new ConcurrentHashMap<>();
	}

	default Statistic s() {
		return Stats.IO_STATS.computeIfAbsent(this, IO::trace);
	}

	default void statistic(Statistic s) {
		if (null == s) Stats.IO_STATS.remove(this);
		else Stats.IO_STATS.put(this, s);
	}
}
