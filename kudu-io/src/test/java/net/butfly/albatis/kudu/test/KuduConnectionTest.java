package net.butfly.albatis.kudu.test;

import java.io.IOException;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

import com.google.gson.JsonObject;

import net.butfly.albatis.kudu.KuduConnection;
import net.butfly.albatis.kudu.KuduResult;

/**
 * @Author Naturn
 *
 * @Date 2017年3月20日-下午5:27:11
 *
 * @Version 1.0.0
 *
 * @Email juddersky@gmail.com
 */

public class KuduConnectionTest {

	@Test
	public void test() {
		
		String kuduUri = "kudu://10.60.70.234:7051";
		try(KuduConnection connection = new KuduConnection(kuduUri,Collections.emptyMap());) {
			
//			KuduOutput output = new KuduOutput("", new URISpec(kuduUri), "");
			JsonObject o = new JsonObject();
			o.addProperty("id", 123456789);
			o.addProperty("name", "123456789");
			KuduResult result = new KuduResult(o, connection.client().openTable("my_first_table"));
			Assert.assertNotNull(result.forWrite());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
