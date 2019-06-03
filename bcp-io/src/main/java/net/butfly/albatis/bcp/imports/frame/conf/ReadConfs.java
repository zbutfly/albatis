package net.butfly.albatis.bcp.imports.frame.conf;


import net.butfly.albatis.bcp.imports.frame.struct.FieldsInfo;
import net.butfly.albatis.bcp.imports.frame.struct.KernelInfo;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

/**
 * nothing.
 *
 * @author : kqlu
 * @version :
 * @code : @since : Created in 20:31 2019/2/28
 */
public class ReadConfs {
	private static String INTUT_PROTOCOL = "//INPUT/PROCOTOL";
	private static String INTUT_PATH = "//INPUT/PATH";
	private static String INTUT_LINESPLIT = "//INPUT/LINESPLIT";
	private static String INTUT_FIELDSPLIT = "//INPUT/FILEDSPLIT";
	private static String INTUT_DATAENAME = "//INPUT/DATAENAME";
	private static String INTUT_DATACNAME = "//INPUT/DATACNAME";
	private static String INTUT_FIELDS = "//INPUT/FIELDS/ITEM";
	private static String OUTPUT_TYPE = "//OUTPUT/TYPE";
	private static String OUTPUT_PATH = "//OUTPUT/PATH";
	private static String OUTPUT_PERSENT = "//OUTPUT/PERSENT";

	public static List<KernelInfo> getKernelInfos(String fileOrDir) throws DocumentException {
		File f = new File(fileOrDir);
		List<KernelInfo> kernelInfos = new LinkedList<>();
		if (f.isFile()) AddNode(kernelInfos, getKernelInfo(fileOrDir));
		else if (f.isDirectory())
			for (File tmpf : f.listFiles()) if (tmpf.isFile()) AddNode(kernelInfos, getKernelInfo(tmpf.getAbsolutePath()));
		return kernelInfos;
	}

	private static KernelInfo getKernelInfo(String file) throws DocumentException {
		Document document = null;
		document = new SAXReader().read(file);
		KernelInfo kernelInfo = new KernelInfo();
		kernelInfo.setInputPath(document.selectSingleNode(INTUT_PATH).getText());
		kernelInfo.setInputProtocol(document.selectSingleNode(INTUT_PROTOCOL).getText());
		// 后面使用readline方法，解析源文件，这项配置对于输入文件无效，
		kernelInfo.setIntputLineSplit(document.selectSingleNode(INTUT_LINESPLIT).getText());
		kernelInfo.setIntputFiledSplit(document.selectSingleNode(INTUT_FIELDSPLIT).getText());
		kernelInfo.setInputDataEName(document.selectSingleNode(INTUT_DATAENAME).getText());
		kernelInfo.setInputDataCName(document.selectSingleNode(INTUT_DATACNAME).getText());
		@SuppressWarnings("unchecked")
        List<Node> nodes = document.selectNodes(INTUT_FIELDS);
		for (Node node : nodes) {
			Element element = (Element) node;
			kernelInfo.addFields(new FieldsInfo(element.attributeValue("from"), //
					element.attributeValue("comment"), //
					element.attributeValue("trans"), //
					element.attributeValue("to"), //
					element.attributeValue("output"), //
					element.attributeValue("mix")));
		}
		kernelInfo.setOutputType(document.selectSingleNode(OUTPUT_TYPE).getText());
		kernelInfo.setOutputPath(document.selectSingleNode(OUTPUT_PATH).getText());
		kernelInfo.setOutputRate(document.selectSingleNode(OUTPUT_PERSENT).getText());
		return kernelInfo;
	}

	private static void AddNode(List<KernelInfo> toList, KernelInfo node) {
		if (node != null) {
			toList.add(node);
		}
	}
}
