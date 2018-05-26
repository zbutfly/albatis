package net.butfly.albatis.ddl.fields;

import net.butfly.albatis.ddl.ValType;

/**
 * @author zx
 *
 */
public class FieldDesc {
	public static final String DEFAULT_FULLTEXT_NAME = "fullText";
	/**
	 * qualifier, maybe include cf/prefix and so on.
	 */
	public final String name;
	public final ValType type;
	public final boolean rowkey;
	public final boolean unique;
	// extra
	private boolean nested = false;
	private String fulltext = null;
	private int segmode = 0;

	public FieldDesc(String name, ValType type, boolean rowkey, boolean unique) {
		super();
		this.name = name;
		this.type = type;
		this.rowkey = rowkey;
		this.unique = unique;
	}

	public FieldDesc(String name, ValType type, boolean rowkey) {
		this(name, type, rowkey, false);
	}

	public FieldDesc(String name, ValType type) {
		this(name, type, false);
	}

	@Override
	public String toString() {
		String s = name + "[" + type.toString();
		if (rowkey) s += ",KEY";
		if (0 != segmode) {
			s += ",TOKEN:";
			switch (segmode) {
			case SEG_MODE_EN:
				s += "EN";
				break;
			case SEG_MODE_CH_IK:
				s += "CH_IK";
				break;
			default:
				throw new IllegalStateException("Invalid segment mode: " + segmode);
			}
		}
		return s;
	}

	public boolean fulltexted() {
		return null == fulltext;
	}

	public int segmode() {
		return segmode;
	}

	/**
	 * 0:不分词,1:默认英文分词2:中文IK分词
	 */
	public FieldDesc segmode(int segmode) {
		this.segmode = segmode;
		return this;
	}

	/**
	 * 旧定义：0:不分词,1:中文IK分词,2:默认英文分词
	 */
	public FieldDesc segmodeOld(int segmode) {
		switch (segmode) {
		case 0:
			this.segmode = SEG_MODE_NONE;
			break;
		case 1:
			this.segmode = SEG_MODE_CH_IK;
			break;
		case 2:
			this.segmode = SEG_MODE_EN;
			break;
		}
		return this;
	}

	public boolean nested() {
		return nested;
	}

	public FieldDesc nested(boolean nested) {
		this.nested = nested;
		return this;
	}

	public String fulltext() {
		return fulltext;
	}

	public FieldDesc fulltext(String fulltext) {
		this.fulltext = fulltext;
		return this;
	}

	// 0:不分词
	public final static int SEG_MODE_NONE = 0;
	// 1:默认英文分词
	public final static int SEG_MODE_EN = 1;
	// 2:中文IK分词
	public final static int SEG_MODE_CH_IK = 2;
}
