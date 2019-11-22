package net.butfly.albatis.kerberos;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.IOs;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.kerberos.huawei.LoginUtil;

public class KerberosLoader {

	protected final String KERBEROS_CONF_PATH;
	protected final String JAAS_CONF = Configs.gets("jaas.conf.name", "jaas.conf");
	protected final String KRB5_CONF = Configs.gets("krb5.conf", "krb5.conf");
	protected final String KERBEROS_PROPERTIES = "kerberos.properties";
	protected final Properties KERBEROS_PROPS = new Properties();
	protected boolean kerberosEnable = false;
	
	public KerberosLoader(String kerberosConfPath) {
		KERBEROS_CONF_PATH = kerberosConfPath;
	}

	public void load() throws IOException {
		if (KERBEROS_CONF_PATH == null) return;
		File kerberosConfigR = new File(KERBEROS_CONF_PATH);
		String[] files = kerberosConfigR.list();
		List<String> fileList = Colls.list(files);
		try {
			KERBEROS_PROPS.load(IOs.openFile(KERBEROS_CONF_PATH + KERBEROS_PROPERTIES));
		} catch (IOException e) {
			throw new RuntimeException("load KERBEROS_PROP error!", e);
			// logger.error("load KERBEROS_PROP error!", e);
		}
		try {
			LoginUtil.setKrb5Config(KERBEROS_CONF_PATH + KRB5_CONF);
		} catch (IOException e) {
			throw e;
		}
		System.setProperty("java.security.auth.login.config", KERBEROS_CONF_PATH + JAAS_CONF);
		String ret = System.getProperty("java.security.auth.login.config");
		if (ret == null)
        {
            throw new IOException("java.security.auth.login.config" + " is null.");
        }
		kerberosEnable = true;
	}

	public boolean kerberosEnable() {
		return KERBEROS_CONF_PATH != null && kerberosEnable;
	}
}
