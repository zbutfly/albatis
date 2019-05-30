package net.butfly.albatis.bcp.utils;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class XmlUtil {

    public static List<String> readXml(String path) {
        List<String> fields = new ArrayList<>();
        SAXReader reader = new SAXReader();
        try {
            Document document = reader.read(new File(path));
            Element rootElement = document.getRootElement();
            Element dataSet = rootElement.element("DATASET").element("DATA").element("DATASET").element("DATA").element("DATASET").element("DATA");
            Iterator it = dataSet.elementIterator();
            while (it.hasNext()) {
                Element bookChild = (Element) it.next();
                fields.add(bookChild.attribute("key").getValue());
            }
        } catch (DocumentException e) {
            e.printStackTrace();
        }
        return fields;
    }

}
