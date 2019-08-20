import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Config;

import java.io.IOException;
import java.util.Date;


public class ConnectionTest {

	public static void main(String[] args)  {
		t1();
	}

	public static void t1()  {
		Object date = new Date();
		Date dd = (Date) date;

		String uri = "odps:http://service.cn-hangzhou-zjga-d01.odps.yun.zj/api?project=appnull_jgdsj&accessId=yEcmh3twHXiabDFh&accessKey=gfrasfsd&tunnelUrl=http://dt.cn-hangzhou-zjga-d01.odps.yun.zj";
		URISpec uriSpec = new URISpec(uri);
		String u = uriSpec.toString();
		String end_point = uri.substring(uri.indexOf(":")+1,uri.indexOf("?"));
		String pro = uriSpec.getParameter("accessId");
		System.out.println(end_point);

	}

}
