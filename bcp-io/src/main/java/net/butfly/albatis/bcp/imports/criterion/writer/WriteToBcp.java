package net.butfly.albatis.bcp.imports.criterion.writer;

import java.io.*;

/**
 * nothing.
 *
 * @author : kqlu
 * @version :
 * @code : @since : Created in 20:16 2019/2/28
 */
public class WriteToBcp implements AutoCloseable {
	private BufferedWriter out = null;

	public String getFileName() {
		return fileName;
	}

	private String fileName = "WriteToBcp_demo";
	private String fieldSplit = "\t";
	private String code = "UTF-8";

	public WriteToBcp(String fileName, String fieldSplit, String code) {
		this.fileName = fileName;
		this.fieldSplit = fieldSplit;
		this.code = code;
	}

	public void write(String[] fileds) {
		if (out == null) {
			try {
				out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName), code));
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
				return;
			} catch (FileNotFoundException e) {
				e.printStackTrace();
				return;
			}
		}

		StringBuilder sb = new StringBuilder();
		for (String str : fileds) {
			sb.append(str).append(fieldSplit);
		}
		sb.deleteCharAt(sb.length() - 1);
		sb.append("\r\n");

		try {
			out.write(sb.toString());
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void close() {
		if (out != null) try {
			out.close();
		} catch (IOException e) {}
	}
}
