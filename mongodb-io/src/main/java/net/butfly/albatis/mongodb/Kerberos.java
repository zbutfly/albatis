package net.butfly.albatis.mongodb;

import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.IOs;
import net.butfly.albatis.kerberos.KerberosLoader;

import java.io.File;
import java.io.IOException;

public class Kerberos extends KerberosLoader {
	private final String KRB5_REALM = Configs.gets("krb5.realm", "krb5.realm");
	private final String KRB5_KDC = Configs.gets("krb5.kdc", "krb5.kdc");
	private final String KRB5_USER = Configs.gets("krb5.user", "krb5.user");
	private String user;
	public Kerberos(String kerberosConfPath) {
		super(kerberosConfPath);
		try {
			KERBEROS_PROPS.load(IOs.openFile(kerberosConfPath + KERBEROS_PROPERTIES));
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.setProperty("java.security.krb5.conf", new File(kerberosConfPath,KRB5_CONF).getPath());
		System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
		System.setProperty("java.security.auth.login.config",new File(kerberosConfPath,JAAS_CONF).getPath());
		System.setProperty("java.security.krb5.realm",KERBEROS_PROPS.getProperty(KRB5_REALM));
		System.setProperty("java.security.krb5.kdc",KERBEROS_PROPS.getProperty(KRB5_KDC));
		user = KERBEROS_PROPS.getProperty(KRB5_USER);
	}
	
	public String getUser() {
		return user;
	}
}
