package net.butfly.albatis.bcp.imports.frame.reader.local;


import net.butfly.albatis.bcp.imports.frame.reader.ReadFromFile;

import java.io.*;

/**
 * nothing.
 *
 * @author : kqlu
 * @version :
 * @code :
 * @since : Created in 15:32 2019/3/1
 */
public class ReadFromLocalFile extends ReadFromFile {

	private BufferedReader in = null;
	private String buff = "";

	@Override
	public boolean load(String path) {
		File f = new File(path);
		if (!f.exists() || ! f.isFile() || !init(path)){
			return false;
		}
		return true;
	}

	private boolean init(String fileName){
		try {
			in = new BufferedReader(new InputStreamReader(new FileInputStream(fileName), "utf-8"));
			return true;
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			return false;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public boolean hasNext() {
		try {
			buff = in.readLine();
			if (buff == null){
				in.close();
				return false;
			}
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}

	}

	@Override
	public String[] next() {
		return buff.split(fieldSplit, -1);
	}
}
