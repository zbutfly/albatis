package net.butfly.albatis.bcp.imports.trans;

import java.io.*;

/**
 * @author : zhuqh
 * @since : Created in 2019/3/23
 */
public class WriteToNb implements AutoCloseable {
	private Writer out = null;

	public String getFileName() {
		return fileName;
	}

	private String fileName = "WriteToNb";
	private String code = "UTF-8";

	public WriteToNb(String fileName, String code) {
		this.fileName = fileName;
		this.code = code;
	}

	public void write(String... fields) throws IOException {
		if (out == null) {
			out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName), code));
			out.flush();
		}

		StringBuilder sb = new StringBuilder();
		for (String str : fields) sb.append(str).append("\r\n");
		sb.deleteCharAt(sb.length() - 1);
		sb.append("\r\n");

		out.write(sb.toString());
	}

	@Override
	public void close() throws IOException {
		if (out != null) out.close();
	}

	public static void main(String[] args) throws IOException {
		WriteToNb writeToBcp = new WriteToNb("C:\\Users\\zhuqh\\Desktop\\test1.nb", "UTF-8");
		String[] fields = { "13040400000020180500008", "R1304040000002018050008", "郭良" };
		writeToBcp.write(fields);
		writeToBcp.close();
	}
}
