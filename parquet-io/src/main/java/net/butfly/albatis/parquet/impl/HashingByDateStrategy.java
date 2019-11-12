package net.butfly.albatis.parquet.impl;

import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.fs.Path;

public class HashingByDateStrategy implements HashingStrategy {
	private static final SimpleDateFormat f = new SimpleDateFormat(//
			"yyyy" + Path.SEPARATOR_CHAR + "MM" + Path.SEPARATOR_CHAR + "dd" + Path.SEPARATOR_CHAR);
	private final String parsingFormat;
	private final SimpleDateFormat p;

	public HashingByDateStrategy(String parsingFormat) {
		super();
		this.parsingFormat = parsingFormat;
		this.p = new SimpleDateFormat(parsingFormat);
	}

	@Override
	public String hashing(Object value) {
		if (null == value) throw new IllegalArgumentException("Parsing hashing strategy failed on value null.");
		Class<? extends Object> c = value.getClass();
		if (Date.class.isAssignableFrom(c)) return f.format(value);
		if (Number.class.isAssignableFrom(c)) return f.format(new Date(((Number) value).longValue()));
		if (CharSequence.class.isAssignableFrom(c)) try {
			return f.format(p.parse(value.toString()));
		} catch (ParseException e) {
			throw new IllegalArgumentException("Parsing hashing strategy  with format [" + parsingFormat + "] failed on value: " + value
					.toString());
		}
		throw new IllegalArgumentException("Parsing hashing strategy failed on value: " + value.toString());
	}
}
