package net.butfly.albatis.bcp.imports.frame.reader;

/**
 * nothing.
 *
 * @author : kqlu
 * @version :
 * @code :
 * @since : Created in 11:26 2019/3/1
 */
public abstract class ReadFromFile {
	protected String lineSplit = "\r\n";
	protected String fieldSplit = "\t";
	public void setLineSplit(String lineSplit) {
		this.lineSplit = lineSplit;
	}

	public void setFieldSplit(String fieldSplit) {
		this.fieldSplit = fieldSplit;
	}
	public abstract boolean load(String path);
	public abstract boolean hasNext();
	public abstract String[] next();
}
