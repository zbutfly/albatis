package net.butfly.albatis.activemq;

import org.apache.activemq.ActiveMQConnectionFactory;

import net.butfly.albacore.io.URISpec;

public class ActivemqConfig {

	public static void config(ActiveMQConnectionFactory factory, URISpec uri) {
		if (Boolean.valueOf(uri.fetchParameter("useAsyncSend", "false")))
			factory.setUseAsyncSend(true);
		if (null != uri.getParameter("producerWindowSize")) {
			factory.setProducerWindowSize(Integer.parseInt(uri.fetchParameter("producerWindowSize")));
		}
	}
}
