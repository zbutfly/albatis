package net.butfly.albatis.io.format;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.butfly.albacore.utils.Configs;
import net.butfly.albatis.io.Rmap;
import net.butfly.alserdes.SerDes;
import net.butfly.alserdes.SerDes.MapSerDes;

@SerDes.As("text")
@SerDes.As(value = "texts", list = true)
public class JudgeDocumentFormat implements MapSerDes<Rmap>{
	
	
	private  final String contentField = Configs.get("input.contentField");
	private  final String demandantField = Configs.get("input.demandantField");
	private  final String defendantField = Configs.get("input.defendantField");
	private  final String xmField = Configs.get("text.xmField");
	private  final String crsqField = Configs.get("text.csrqField");
	

	@Override
	public  List<Map<String, Object>> desers(Rmap rmap) {
		List<Map<String, Object>> RMaps = new ArrayList<Map<String,Object>>();
		String regExp = "(?<=委托代理人)(.*?)(?=被告)";
		String regExpDate = "[0-9]{4}年[0-9]{1,2}月[0-9]{1,2}日出?生";
		String regExp2 = "(?<=执行人:)(.*?)(?=\\,)";
		String valueString = rmap.get(contentField).toString().replaceAll(regExp, "");
		
		Pattern p = Pattern.compile(regExpDate);
        Matcher matcher = p.matcher(valueString);
		List<String> crsq_list = new ArrayList<String>();
		List<String> xm_list = new ArrayList<String>();
        while (matcher.find()) {
        	crsq_list.add(matcher.group());
        }
        if(crsq_list.size()!=0) {
        	if(!rmap.get(demandantField).toString().isEmpty() && !rmap.get(defendantField).toString().isEmpty()) {
        		String[] s1 = rmap.get(demandantField).toString().split("、 ");
        		String[] s2 = rmap.get(defendantField).toString().split("、 ");
        		for(String string : s1) {
        			if(string.length() < 5) xm_list.add(string);
        		}
        		for(String string : s2) {
        			if(string.length() < 5) xm_list.add(string);
        		}
        	}else {
        		Pattern p1 = Pattern.compile(regExp2);
                Matcher matcher1 = p1.matcher(valueString);
                while (matcher1.find()) {
                	xm_list.add(matcher1.group());
                }
			}
        	
        }
        if(xm_list.size() == crsq_list.size()) {
        	for(int i = 0; i<xm_list.size(); i++) {
        		String xmString = xm_list.get(i);
        		String crsq = crsq_list.get(i).replace("出生", "");
        		if("".equals(xmString)||null==xmString||"".equals(crsq)||null==crsq){continue;}
        		Rmap rr = new Rmap();
    			rr.table(rmap.table());
    			rr.key(rmap.key());
    			rr.putAll(rmap);
    			rr.remove(contentField);
    			rr.remove(demandantField);
    			rr.remove(defendantField);
    			rr.put(xmField, xmString);
    			rr.put(crsqField, crsq);
    			RMaps.add(rr);
        	}
        }	

		return RMaps;
	}
	


}
