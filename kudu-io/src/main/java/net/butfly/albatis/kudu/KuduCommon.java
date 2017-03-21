package net.butfly.albatis.kudu;

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

/**
 * @Author Naturn
 *
 * @Date 2016年12月8日-下午5:18:42
 *
 * @Version 1.0.0
 *
 * @Email juddersky@gmail.com
 */

public class KuduCommon {

    private static final TimeZone TIMEZONE = TimeZone.getTimeZone("GMT+8");

    public static final SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static final PartialRow generateColumnData(Type type, PartialRow row, String name, Object o) {
        switch (type) {
        case INT8:
            row.addByte(name, (byte) o);
            break;
        case INT16:
            row.addShort(name, Short.parseShort(o.toString()));
            break;
        case INT32:
            row.addInt(name, Integer.parseInt(o.toString()));
            break;
        case INT64:
            long lv = 0;
            if (o instanceof Date) {
                lv = ((Date) o).getTime()*1000;
            } else {
                lv = Long.parseLong(o.toString());
            }
            row.addLong(name, lv);
            break;
        case UNIXTIME_MICROS:
            long temp = 0;
            if (o instanceof Date) {
                temp = ((Date) o).getTime();
            } else {
                temp = Long.parseLong(o.toString());
            }
            row.addLong(name, temp);
            break;
        case STRING:
            row.addString(name, o == null ? "" : o.toString());
            break;
        case BOOL:
            row.addBoolean(name, Boolean.parseBoolean(o.toString()));
            break;
        case FLOAT:
            row.addFloat(name, Float.parseFloat(o.toString()));
            break;
        case DOUBLE:
            row.addDouble(name, Double.parseDouble(o.toString()));
            break;
        default:
            row.addString(name, o == null ? "" : o.toString());
            break;
        }
        return row;
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
            c.set(c.get(Calendar.YEAR), i, c.getActualMaximum(Calendar.DAY_OF_MONTH),
                    c.getActualMaximum(Calendar.HOUR_OF_DAY), c.getActualMaximum(Calendar.MINUTE),
                    c.getActualMaximum(Calendar.SECOND));
            Date upper = c.getTime();
            months.put(low, upper);
        }
        return months;
    }

    /**
     *
     * @param low
     *            (ps.2016-xx)
     * @param upper
     *            (ps.2016-xx)
     * 
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

        c.set(upperYear, upperMonth - 1, 1, c.getActualMaximum(Calendar.HOUR_OF_DAY),
                c.getActualMaximum(Calendar.MINUTE), c.getActualMaximum(Calendar.SECOND));
        c.set(Calendar.DAY_OF_MONTH, c.getActualMaximum(Calendar.DAY_OF_MONTH));
        c.set(Calendar.MILLISECOND, c.getActualMaximum(Calendar.MILLISECOND));
        Date upperDate = c.getTime();
        System.out.println(SDF.format(upperDate));
        ///
        c.set(lowYear, lowMonth - 1, 1, 0, 0, 0);
        dates.add(c.getTime());
        System.out.println(SDF.format(c.getTime()));

        c.set(c.get(Calendar.YEAR), c.get(Calendar.MONTH), c.getActualMaximum(Calendar.DAY_OF_MONTH),
                c.getActualMaximum(Calendar.HOUR_OF_DAY), c.getActualMaximum(Calendar.MINUTE),
                c.getActualMaximum(Calendar.SECOND));
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
     *
     * @param low
     *            (ps.2016)
     * @param upper
     *            (ps.2017)
     * 
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
     *
     * @param low
     *            (ps.2016-xx-xx)
     * @param upper
     *            (ps.2017-xx-xx)
     * 
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
        c.set(upperYear, upperMonth - 1, upperDay, c.getActualMaximum(Calendar.HOUR_OF_DAY),
                c.getActualMaximum(Calendar.MINUTE), c.getActualMaximum(Calendar.SECOND));
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

//    public static void main(String[] args) {
//        splitByDay("2016-12-28", "2017-1-2").forEach(p -> {
//            System.out.println(SDF.format(p));
//        });
//    }

}
