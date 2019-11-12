package net.butfly.albatis.parquet.impl;

import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class HashingByDateStrategy implements HashingStrategy {
	private final String parsingFormat;
	private final SimpleDateFormat parsing;
	private final SimpleDateFormat hashing;

	public HashingByDateStrategy(String hashingFormat, String parsingFormat) {
		super();
		this.parsingFormat = parsingFormat;
		this.parsing = null == parsingFormat ? null : new SimpleDateFormat(parsingFormat);
		this.hashing = new SimpleDateFormat(hashingFormat);
	}

	@Override
	public String hashing(Object value) {
		if (null == value) throw new IllegalArgumentException("Hashing failed on value null.");
		Class<? extends Object> c = value.getClass();
		if (Date.class.isAssignableFrom(c)) return hashing.format(value);
		if (Number.class.isAssignableFrom(c)) return hashing.format(new Date(((Number) value).longValue()));
		if (CharSequence.class.isAssignableFrom(c)) if (null == parsing) //
			throw new IllegalArgumentException("Hashing failed for string value without parseing format.");
		else try {
			return hashing.format(parsing.parse(value.toString()));
		} catch (ParseException e) {
			throw new IllegalArgumentException("Hashing failed with format [" + parsingFormat + "] failed on value: " + value.toString());
		}
		throw new IllegalArgumentException("Hashing failed on value: " + value.toString());
	}
}
