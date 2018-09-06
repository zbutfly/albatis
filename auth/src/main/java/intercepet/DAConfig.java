package intercepet;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import net.butfly.albacore.utils.logger.Loggable;
import org.apache.cxf.endpoint.Client;
import org.apache.cxf.jaxws.endpoint.dynamic.JaxWsDynamicClientFactory;
import org.apache.cxf.transport.http.HTTPConduit;
import org.apache.cxf.transports.http.configuration.HTTPClientPolicy;

public class DAConfig implements Loggable {
	private static String menulfUrl;
	private static String methodName;
	public static void setMenulfUrl(String url) {
		menulfUrl = url;
	}
	
	public static void setMethodName(String name) {
		methodName = name;
	}
	
	protected static JSONArray filter(String jsonStr) {
		if (menulfUrl == null || menulfUrl.isEmpty() || methodName == null || methodName.isEmpty()) {
			System.out.println("menulfUrl or methodName is none!");
			return null;
		}
    	try {
    		Object[] results = getServiceResult(jsonStr);
    		if (results != null && results.length > 0) {
    			Object result = results[0];
    			JSONObject json = JSON.parseObject(result.toString());
    			return json.get("responseObject") == null ? new JSONArray():(JSONArray) json.get("responseObject");
    		}
		} catch (Exception e) {
			throw new RuntimeException("getServiceResult error: " + e);
		}
		return null;
    }
    
    private static Object[] getServiceResult(String json) throws Exception {
        JaxWsDynamicClientFactory dcf = JaxWsDynamicClientFactory.newInstance();
        Client client = dcf.createClient(menulfUrl);
        HTTPConduit http = (HTTPConduit) client.getConduit();
        http.getClient().setReceiveTimeout(0);
        HTTPClientPolicy httpClientPolicy = new HTTPClientPolicy();
        httpClientPolicy.setConnectionTimeout(6000000);// 连接超时(毫秒)
        httpClientPolicy.setAllowChunking(false);// 取消块编码
        httpClientPolicy.setReceiveTimeout(6000000);// 响应超时(毫秒)
        return client.invoke(methodName, json);
    }
}