package net.butfly.albatis.bcp.imports.frame.struct;

import net.butfly.albacore.utils.logger.Logger;

import java.util.LinkedList;
import java.util.List;

/**
 * nothing.
 *
 * @author : kqlu
 * @version :
 * @code : @since : Created in 20:21 2019/2/28
 */
public class KernelInfo {
	private static final Logger logger = Logger.getLogger(KernelInfo.class);

	public String getInputProtocol() { return inputProtocol; }

	public void setInputProtocol(String inputProtocol) { this.inputProtocol = inputProtocol; }

	public String getInputPath() { return inputPath; }

	public void setInputPath(String inputPath) { this.inputPath = inputPath; }

	public String getInputDataEName() { return inputDataEName; }

	public void setInputDataEName(String inputDataEName) { this.inputDataEName = inputDataEName; }

	public String getInputDataCName() { return inputDataCName; }

	public void setInputDataCName(String inputDataCName) { this.inputDataCName = inputDataCName; }

	public List<FieldsInfo> getFields() { return fields; }

	public void addFields(FieldsInfo field) {
		if (this.fields == null) {
			this.fields = new LinkedList<>();
		}
		this.fields.add(field);
	}

	public String getOutputType() { return outputType; }

	public void setOutputType(String outputType) { this.outputType = outputType; }

	public int getOutputRate() { return outputRate; }

	public void setOutputRate(String outputRate) { this.outputRate = Integer.valueOf(outputRate); }

	public String getIntputLineSplit() { return intputLineSplit; }

	public void setIntputLineSplit(String intputLineSplit) { this.intputLineSplit = intputLineSplit; }

	public String getIntputFiledSplit() { return intputFiledSplit; }

	public void setIntputFiledSplit(String intputFiledSplit) { this.intputFiledSplit = intputFiledSplit; }

	public String getOutputPath() { return outputPath; }

	public void setOutputPath(String outputPath) { this.outputPath = outputPath; }

	private String inputProtocol = "";
	private String inputPath = "";
	private String intputLineSplit = "";
	private String intputFiledSplit = "";
	private String inputDataEName = "";
	private String inputDataCName = "";
	private List<FieldsInfo> fields = null;
	private String outputType = "";
	private String outputPath = "";
	// private String outputSplit = "";
	private int outputRate;

	public void print() {
		logger.debug("inputProtocol: " + inputProtocol);
		logger.debug("inputPath: " + inputPath);
		logger.debug("intputLineSplit: " + intputLineSplit);
		logger.debug("intputFiledSplit: " + intputFiledSplit);
		logger.debug("inputDataEName: " + inputDataEName);
		logger.debug("inputDataCName: " + inputDataCName);
		for (FieldsInfo field : fields) {
			logger.debug("\tfrom " + field.getSrcField());
			logger.debug("\tcomment " + field.getComment());
			logger.debug("\ttrans " + field.getTransFunction());
			logger.debug("\tto " + field.getDestField());
			logger.debug("\trate " + field.getOutputRate());
			logger.debug("\tmixrate " + field.getMixRate());
		}
		logger.debug("outputType: " + outputType);
		logger.debug("outputPath: " + outputPath);
		logger.debug("outputRate: " + outputRate);
	}

}
