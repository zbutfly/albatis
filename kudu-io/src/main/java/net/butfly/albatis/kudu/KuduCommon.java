package net.butfly.albatis.kudu;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TimeZone;
import java.util.TreeSet;

import org.apache.kudu.Type;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowResult;

import net.butfly.albatis.ddl.vals.ValType;

public class KuduCommon {
	// private static final Logger logger = Logger.getLogger(KuduCommon.class);
	private static final TimeZone TIMEZONE = TimeZone.getTimeZone("GMT+8");
	public static final SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static final PartialRow generateColumnData(Type type, int index, PartialRow row, Object o) {
		switch (type) {
			case STRING:
				row.addString(index, o.toString());
				break;
			case INT8:
				if (o instanceof Number) row.addByte(index, ((Number) o).byteValue());
				else if (o instanceof CharSequence) row.addByte(index, Byte.parseByte(((CharSequence) o).toString()));
				break;
			case INT16:
				if (o instanceof Number) row.addShort(index, ((Number) o).shortValue());
				else if (o instanceof CharSequence)
					row.addShort(index, Short.parseShort(((CharSequence) o).toString()));
				break;
			case INT32:
				if (o instanceof Number) row.addInt(index, ((Number) o).intValue());
				else if (o instanceof CharSequence) row.addInt(index, Integer.parseInt(((CharSequence) o).toString()));
				break;
			case INT64:
				if (o instanceof Number) row.addLong(index, ((Number) o).longValue());
				else if (o instanceof Timestamp) row.addLong(index, ((Timestamp) o).getTime());
				else if (o instanceof Date) row.addLong(index, ((Date) o).getTime());
				else if (o instanceof CharSequence) row.addLong(index, Long.parseLong(((CharSequence) o).toString()));
				break;
			case FLOAT:
				if (o instanceof Number) row.addFloat(index, ((Number) o).floatValue());
				else if (o instanceof CharSequence)
					row.addFloat(index, Float.parseFloat(((CharSequence) o).toString()));
				break;
			case DOUBLE:
				if (o instanceof Number) row.addDouble(index, ((Number) o).doubleValue());
				else if (o instanceof CharSequence)
					row.addDouble(index, Double.parseDouble(((CharSequence) o).toString()));
				break;
			case UNIXTIME_MICROS: // datetime
				Timestamp ts = null;
				if (o instanceof Date) ts = new Timestamp(((Date) o).getTime());
				else if (o instanceof Number) ts = new Timestamp(((Number) o).longValue());
				else if (o instanceof CharSequence) ts = new Timestamp(Long.parseLong(((CharSequence) o).toString()));
				if (null != ts) row.addTimestamp(index, ts);
				break;
			case BOOL:
				if (o instanceof Boolean) row.addBoolean(index, ((Boolean) o).booleanValue());
				else if (o instanceof Number) row.addBoolean(index, 0 == ((Number) o).doubleValue());
				else if (o instanceof CharSequence)
					row.addBoolean(index, Boolean.parseBoolean(((CharSequence) o).toString()));
				break;
			case BINARY:
				if (o instanceof byte[]) row.addBinary(index, (byte[]) o);
				else if (o instanceof CharSequence) row.addBinary(index, ((CharSequence) o).toString().getBytes());
			case DECIMAL:
				if (o instanceof BigDecimal) row.addDecimal(index, (BigDecimal) o);
				else if (o instanceof Number)
					row.addDecimal(index, new BigDecimal(((Number) o).toString())); // dangerous!!!
				else if (o instanceof CharSequence)
					row.addDecimal(index, new BigDecimal(((CharSequence) o).toString()));
				break;
		}
		return row;
	}

	public static final ValType valType(Type type) {
		switch (type) {
			case STRING:
				return ValType.STR;
			case INT8:
				return ValType.BYTE;
			case INT16:
				return ValType.SHORT;
			case INT32:
				return ValType.INT;
			case INT64:
				return ValType.LONG;
			case FLOAT:
				return ValType.FLOAT;
			case DOUBLE:
			case DECIMAL:
				return ValType.DOUBLE;
			case UNIXTIME_MICROS: // datetime
				return ValType.DATE;
			case BOOL:
				return ValType.BOOL;
			case BINARY:
				return ValType.BIN;
		}
		throw new IllegalArgumentException();
	}

	public static final Type kuduType(String type) {
		switch (type) {
			case "string":
				return Type.STRING;
			case "date":
				return Type.UNIXTIME_MICROS;
			case "int":
				return Type.INT8;
			case "long":
				return Type.INT64;
			case "double":
				return Type.DOUBLE;
			default:
				return Type.STRING;
		}
	}

	public static final Map<Date, Date> splitParByMonth() {
		Map<Date, Date> months = new HashMap<>();
		Calendar c = Calendar.getInstance(TIMEZONE);
		for (int i = 0; i < 12; i++) {
			c.set(c.get(Calendar.YEAR), i, 1, 0, 0, 0);
			Date low = c.getTime();
			c.set(c.get(Calendar.YEAR), i, c.getActualMaximum(Calendar.DAY_OF_MONTH), c.getActualMaximum(Calendar.HOUR_OF_DAY), c
					.getActualMaximum(Calendar.MINUTE), c.getActualMaximum(Calendar.SECOND));
			Date upper = c.getTime();
			months.put(low, upper);
		}
		return months;
	}

	/**
	 * @param low   (ps.2016-xx)
	 * @param upper (ps.2016-xx)
	 */
	public static final Set<Date> splitByMonth(String low, String upper) {

		SortedSet<Date> dates = new TreeSet<>();
		String[] lows = low.split("-");
		int lowYear = Integer.parseInt(lows[0]);
		int lowMonth = Integer.parseInt(lows[1]);

		String[] uppers = upper.split("-");
		int upperYear = Integer.parseInt(uppers[0]);
		int upperMonth = Integer.parseInt(uppers[1]);

		Calendar c = Calendar.getInstance(TIMEZONE);
		c.set(Calendar.MILLISECOND, 0);

		c.set(upperYear, upperMonth - 1, 1, c.getActualMaximum(Calendar.HOUR_OF_DAY), c.getActualMaximum(Calendar.MINUTE), c
				.getActualMaximum(Calendar.SECOND));
		c.set(Calendar.DAY_OF_MONTH, c.getActualMaximum(Calendar.DAY_OF_MONTH));
		c.set(Calendar.MILLISECOND, c.getActualMaximum(Calendar.MILLISECOND));
		Date upperDate = c.getTime();
		System.out.println(SDF.format(upperDate));
		///
		c.set(lowYear, lowMonth - 1, 1, 0, 0, 0);
		dates.add(c.getTime());
		System.out.println(SDF.format(c.getTime()));

		c.set(c.get(Calendar.YEAR), c.get(Calendar.MONTH), c.getActualMaximum(Calendar.DAY_OF_MONTH), c.getActualMaximum(
				Calendar.HOUR_OF_DAY), c.getActualMaximum(Calendar.MINUTE), c.getActualMaximum(Calendar.SECOND));
		c.set(Calendar.MILLISECOND, c.getActualMaximum(Calendar.MILLISECOND));
		dates.add(c.getTime());
		System.out.println(SDF.format(c.getTime()));

		while (c.getTime().compareTo(upperDate) < 0) {
			c.set(Calendar.DAY_OF_MONTH, c.get(Calendar.DAY_OF_MONTH) + 1);
			c.set(Calendar.HOUR_OF_DAY, 0);
			c.set(Calendar.MINUTE, 0);
			c.set(Calendar.SECOND, 0);
			c.set(Calendar.MILLISECOND, 0);
			dates.add(c.getTime());
			c.set(Calendar.DAY_OF_MONTH, c.getActualMaximum(Calendar.DAY_OF_MONTH));
			c.set(Calendar.HOUR_OF_DAY, c.getActualMaximum(Calendar.HOUR_OF_DAY));
			c.set(Calendar.MINUTE, c.getActualMaximum(Calendar.MINUTE));
			c.set(Calendar.SECOND, c.getActualMaximum(Calendar.SECOND));
			c.set(Calendar.MILLISECOND, c.getActualMaximum(Calendar.MILLISECOND));
			dates.add(c.getTime());
		}
		return dates;
	}

	/**
	 * @param low   (ps.2016)
	 * @param upper (ps.2017)
	 */
	public static final Set<Date> splitByYear(String low, String upper) {
		Set<Date> dates = new TreeSet<>();
		int lowYear = Integer.parseInt(low);
		int upperYear = Integer.parseInt(upper);
		for (int i = lowYear; i <= upperYear; i++) {
			Calendar c = Calendar.getInstance(TIMEZONE);
			c.set(i, 0, 1, 0, 0, 0);
			dates.add(c.getTime());
			c.set(i, 11, 31, 23, 59, 59);
			dates.add(c.getTime());
		}
		return dates;
	}

	/**
	 * @param low   (ps.2016-xx-xx)
	 * @param upper (ps.2017-xx-xx)
	 */

	public static final Set<Date> splitByDay(String low, String upper) {

		SortedSet<Date> dates = new TreeSet<>();
		String[] lows = low.split("-");
		int lowYear = Integer.parseInt(lows[0]);
		int lowMonth = Integer.parseInt(lows[1]);
		int lowDay = Integer.parseInt(lows[2]);

		String[] uppers = upper.split("-");
		int upperYear = Integer.parseInt(uppers[0]);
		int upperMonth = Integer.parseInt(uppers[1]);
		int upperDay = Integer.parseInt(uppers[2]);

		Calendar c = Calendar.getInstance(TIMEZONE);
		c.set(Calendar.MILLISECOND, 0);
		c.set(upperYear, upperMonth - 1, upperDay, c.getActualMaximum(Calendar.HOUR_OF_DAY), c.getActualMaximum(Calendar.MINUTE), c
				.getActualMaximum(Calendar.SECOND));
		Date upperDate = c.getTime();

		c.set(lowYear, lowMonth - 1, lowDay, 0, 0, 0);
		dates.add(c.getTime());
		c.set(Calendar.HOUR_OF_DAY, c.getActualMaximum(Calendar.HOUR_OF_DAY));
		c.set(Calendar.MINUTE, c.getActualMaximum(Calendar.MINUTE));
		c.set(Calendar.SECOND, c.getActualMaximum(Calendar.SECOND));
		c.set(Calendar.MILLISECOND, c.getActualMaximum(Calendar.MILLISECOND));
		dates.add(c.getTime());

		while (c.getTime().compareTo(upperDate) < 0) {
			c.set(Calendar.DATE, c.get(Calendar.DATE) + 1);
			c.set(Calendar.HOUR_OF_DAY, 0);
			c.set(Calendar.MINUTE, 0);
			c.set(Calendar.SECOND, 0);
			c.set(Calendar.MILLISECOND, 0);
			dates.add(c.getTime());
			c.set(Calendar.HOUR_OF_DAY, c.getActualMaximum(Calendar.HOUR_OF_DAY));
			c.set(Calendar.MINUTE, c.getActualMaximum(Calendar.MINUTE));
			c.set(Calendar.SECOND, c.getActualMaximum(Calendar.SECOND));
			c.set(Calendar.MILLISECOND, c.getActualMaximum(Calendar.MILLISECOND));
			dates.add(c.getTime());
		}
		return dates;
	}

	public static Object getValue(RowResult row, String name, Type type) {
		switch (type) {
			case INT8:
				if (!row.isNull(name)) return row.getByte(name);
			case INT16:
				if (!row.isNull(name)) return row.getShort(name);
			case INT32:
				if (!row.isNull(name)) return row.getInt(name);
			case INT64:
				if (!row.isNull(name)) return row.getLong(name);
			case UNIXTIME_MICROS:
				Timestamp ts = null;
				if (!row.isNull(name)) ts = row.getTimestamp(name);
				return null == ts ? null : new Date(ts.getTime());
			case STRING:
				if (!row.isNull(name)) return row.getString(name);
			case BOOL:
				if (!row.isNull(name)) return row.getBoolean(name);
			case FLOAT:
				if (!row.isNull(name)) return row.getFloat(name);
			case DOUBLE:
				if (!row.isNull(name)) return row.getDouble(name);
			case BINARY:
				if (!row.isNull(name)) return row.getBinary(name);
			case DECIMAL:
				if (!row.isNull(name)) return row.getDecimal(name);
		}
		return null;
	}
}
