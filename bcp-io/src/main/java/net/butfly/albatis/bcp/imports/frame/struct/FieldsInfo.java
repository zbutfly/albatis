package net.butfly.albatis.bcp.imports.frame.struct;

/**
 * nothing.
 *
 * @author : kqlu
 * @version :
 * @code :
 * @since : Created in 14:39 2019/3/1
 */
public class FieldsInfo {
	public String getSrcField() {
		return srcField;
	}

	public String getComment() {
		return comment;
	}

	public String getDestField() {
		return destField;
	}

	public int getOutputRate() {
		return outputRate;
	}

	public int getMixRate() {
		return mixRate;
	}
	public String getTransFunction() {
		return transFunction;
	}

	private String srcField = "";
	private String comment = "";
	private String destField = "";
	private int outputRate = 0;
	private int mixRate = 0;
	private String transFunction = "";

	public FieldsInfo(String from , String comment, String trans, String to, String outputRate, String mixRate){
		this.srcField = from;
		this.comment = comment;
		this.transFunction = trans == null ? "" : trans;
		this.destField = to;
		this.outputRate = Integer.valueOf(outputRate);
		this.mixRate = Integer.valueOf(mixRate);

	}
}
