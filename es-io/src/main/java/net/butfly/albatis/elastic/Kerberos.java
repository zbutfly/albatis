package net.butfly.albatis.elastic;

import net.butfly.albatis.kerberos.KerberosLoader;

public class Kerberos extends KerberosLoader {

	private String user;
	public Kerberos(String kerberosConfPath) {
		super(kerberosConfPath);
		user = KERBEROS_PROPS.getProperty("es.user");
	}

	public void setKerberosConfig() {
		
	}
	
	public String getUser() {
		return user;
	}
}
