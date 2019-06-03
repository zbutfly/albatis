package net.butfly.albatis.bcp.imports.trans;

import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

/**
 * @author : zhuqh
 * @since : Created in 2019/3/23
 */
public class WriteToXml implements AutoCloseable {
	private Document doc = null;

	public String getOutputFileName() {
		return outputFileName;
	}

	private String outputFileName = "";

	public WriteToXml(String outputFileName) {
		createBaseXml();
		this.outputFileName = outputFileName;
	}

	private void createBaseXml() {
		doc = DocumentHelper.createDocument();
		Element root = doc.addElement("MESSAGE");
		Element element1 = root.addElement("INPUT");
		element1.addAttribute("comment", "输入相关信息");
	}

	public void addOutPutInfo(String outPutPath) {
		Element element = (Element) doc.selectSingleNode("//MESSAGE");
		Element element1 = element.addElement("OUTPUT");
		element1.addAttribute("comment", "输出相关信息");

		Element item1 = element1.addElement("TYPE");
		item1.setText("zip");

		Element item2 = element1.addElement("PATH");
		item2.setText(outPutPath + "zip" + File.separator);

		Element item3 = element1.addElement("PERSENT");
		item3.setText("100");

	}

	public void addFileds(String[][] fileds) {
		if (fileds == null || fileds[0] == null || fileds[0].length != 2) { return; }
		Element element = (Element) doc.selectSingleNode("//MESSAGE/INPUT/FIELDS");
		for (int i = 0; i < fileds.length; i++) {
			Element item = element.addElement("ITEM");
			item.addAttribute("from", fileds[i][0] == null ? "" : fileds[i][0]);
			item.addAttribute("comment", fileds[i][1] == null ? "" : fileds[i][1]);
			item.addAttribute("trans", "");
			item.addAttribute("to", fileds[i][0] == null ? "" : fileds[i][0]);
			item.addAttribute("output", "100");
			item.addAttribute("mix", "0");
		}
	}

	public void addInPutInfo(String path, String bcpName, String bcpCName) {
		Element element = (Element) doc.selectSingleNode("//MESSAGE/INPUT");

		Element item1 = element.addElement("PROCOTOL");
		item1.setText("file");

		Element item2 = element.addElement("PATH");
		item2.setText(path + "nb" + File.separator);

		Element item3 = element.addElement("FILEDSPLIT");
		item3.setText("\\t");

		Element item6 = element.addElement("LINESPLIT");
		item6.addAttribute("comment", "当前版本，配置无效，用于传递输出结果");
		item6.setText("\\n");

		Element item4 = element.addElement("DATAENAME");
		item4.setText(bcpName);

		Element item5 = element.addElement("DATACNAME");
		item5.setText(bcpCName);

		Element element1 = element.addElement("FIELDS");
		element1.addAttribute("comment", "字段映射表");
	}

	@Override
	public void close() throws IOException {
		OutputFormat format = OutputFormat.createPrettyPrint();
		format.setEncoding("UTF-8");
		try (Writer out = new FileWriter(this.outputFileName)) {
			XMLWriter writer = new XMLWriter(out, format);
			try {
				writer.write(doc);
			} finally {
				writer.close();
			}
		}
	}

	public static void main(String[] args) throws IOException {
		try (WriteToXml writeToXml = new WriteToXml("C:\\Users\\zhuqh\\Desktop\\test.xml");) {
			writeToXml.addInPutInfo("./data", "fileName", "中文名");
			writeToXml.addOutPutInfo("./output");
			String[][] fileds = { { "XXZJBH", "信息主键编号" }, { "RYBH", "人员编号" }, { "XM", "姓名" } };
			writeToXml.addFileds(fileds);
		}
	}
}
