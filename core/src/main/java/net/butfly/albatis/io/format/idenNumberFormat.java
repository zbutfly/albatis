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

@SerDes.As("idNum")
@SerDes.As(value = "idNums", list = true)
public class idenNumberFormat implements MapSerDes<Rmap>{
	private  final String jqField = Configs.get("input.jqField");
	private  final String idNumField = Configs.get("idNumber.idNumField");
	
	@Override
	public  List<Map<String, Object>> desers(Rmap rmap) {
		
		List<Map<String, Object>> RMaps = new ArrayList<Map<String,Object>>();
		String regExp = "[0-9]{17}[0-9|x|X]";
		String valueString = rmap.get(jqField).toString();
		Pattern p = Pattern.compile(regExp);
        Matcher matcher = p.matcher(valueString);
  
        while (matcher.find()) {
        	Rmap rr = new Rmap();
			rr.table(rmap.table());
			rr.key(rmap.key());
			rr.putAll(rmap);
			rr.remove(jqField);
			rr.put(idNumField, matcher.group());
			RMaps.add(rr);
        }
		return RMaps;
	}

}
