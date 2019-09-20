package net.butfly.albatis.bcp.imports.criterion.writer;

import net.butfly.albacore.utils.logger.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

/**
 * nothing.
 *
 * @author : kqlu
 * @version :
 * @code : @since : Created in 20:16 2019/2/28
 */
public class WriteToXml {
	private static final Logger logger = Logger.getLogger(WriteToXml.class);
	// private String fileName = "GAB_ZIP_INDEX.xml";
	private Document doc = null;

	public String getOutputFileName() { return outputFileName; }

	private String outputFileName = "";

	public WriteToXml(String outputFileName) {
		createBaseXml();
		this.outputFileName = outputFileName;
	}

	private void createBaseXml() {
		doc = DocumentHelper.createDocument();
		Element root = doc.addElement("MESSAGE");
		addDataSetNode(root, "WA_COMMON_010017", "数据文件索引信息");
	}

	private Element addDataSetNode(Element element, String name, String rmk) {
		Element element1 = element.addElement("DATASET");
		element1.addAttribute("name", name);
		element1.addAttribute("rmk", rmk);
		return element1.addElement("DATA");
	}

	private void addDataSourceItem(Element element, String key, String val, String rmk) {
		// Element element = (Element) doc.selectSingleNode(dataSourceInfoLocate);
		Element item = element.addElement("ITEM");
		item.addAttribute("key", key);
		item.addAttribute("val", val);
		item.addAttribute("rmk", rmk);
	}

	public void addDataSourceInfo(String company, String collectionPlace, String dataSource, String fileCode, String lineSplit,
                                  String fieldSplit) {
		Element element = (Element) doc.selectSingleNode("//MESSAGE/DATASET/DATA");
		element = addDataSetNode(element, "WA_COMMON_010013", "BCP文件描述信息");
		addDataSourceItem(element, "B050016", dataSource, "数据来源");
		addDataSourceItem(element, "F010008", collectionPlace, "数据采集地");
		addDataSourceItem(element, "G020013", company, "产品厂家组织机构代码");
		addDataSourceItem(element, "I010039", fileCode, "可选项，默认为UTF-８，BCP文件编码格式（采用不带格式的编码方式，如：UTF-８无BOM）");
		addDataSourceItem(element, "A010004", "WA_SOURCE_Z002_9967", "数据集代码");
		addDataSourceItem(element, "I010032", fieldSplit, "列分隔符（缺少值时默认为制表符\\t）");
		addDataSourceItem(element, "I010033", lineSplit, "行分隔符（缺少值时默认为换行符\\r\\n）");
	}

	public void addFileds(String[][] fileds) {
		if (fileds == null || fileds[0] == null || fileds[0].length != 2) { return; }
		// Element element = (Element) doc.selectSingleNode(fieldLocate);
		Element element = (Element) doc.selectSingleNode("//MESSAGE/DATASET/DATA/DATASET/DATA");
		if (element == null) {
			logger.debug("please call addDataSourceInfo first.");
			return;
		}
		element = addDataSetNode(element, "WA_COMMON_010015", "BCP文件数据结构");
		for (int i = 0; i < fileds.length; i++) {
			Element item = element.addElement("ITEM");
			item.addAttribute("key", fileds[i][0] == null ? "" : fileds[i][0]);
			item.addAttribute("chn", fileds[i][1] == null ? "" : fileds[i][1]);
			item.addAttribute("eng", fileds[i][0] == null ? "" : fileds[i][0]);
		}
	}

	public void addBcpInfo(String path, String bcpName, String bcpLineNum) {
		Element element = (Element) doc.selectSingleNode("//MESSAGE/DATASET/DATA/DATASET/DATA");
		if (element == null) {
			logger.debug("please call addDataSourceInfo first.");
			return;
		}
		element = addDataSetNode(element, "WA_COMMON_010014", "BCP数据文件信息");
		Element item1 = element.addElement("ITEM");
		item1.addAttribute("key", "H040003");
		item1.addAttribute("val", path);
		item1.addAttribute("rmk", "文件路径");

		Element item2 = element.addElement("ITEM");
		item2.addAttribute("key", "H010020");
		item2.addAttribute("val", bcpName);
		item2.addAttribute("rmk", "文件名");

		Element item3 = element.addElement("ITEM");
		item3.addAttribute("key", "I010034");
		item3.addAttribute("val", bcpLineNum);
		item3.addAttribute("rmk", "记录行数");
	}

	public void flush() {
		try {
			Writer out = new FileWriter(this.outputFileName);
			OutputFormat format = OutputFormat.createPrettyPrint();
			format.setEncoding("UTF-8");

			XMLWriter writer = new XMLWriter(out, format);
			writer.write(doc);
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		WriteToXml writeToXml = new WriteToXml("test.xml");
//		writeToXml.createBaseXml();
		writeToXml.addDataSourceInfo("fenghuo", "330000", "144", "UTF-8", "\t", "\n");
		String[][] fileds = { { "test1", "123" }, { "test2", "234" }, { "test3", "345" } };
		writeToXml.addFileds(fileds);
		writeToXml.addBcpInfo("", "fileName", String.valueOf(5000));
		writeToXml.flush();
	}
}
