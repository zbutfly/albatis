package net.butfly.albatis.hbase.utils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.IOs;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.logger.Logger;

public class Kerberoses {
	private static final Logger logger = Logger.getLogger(Kerberoses.class);
	private static final String LINE_SEPARATOR = System.getProperty("line.separator");
	private static final boolean IS_IBM_JDK = System.getProperty("java.vendor").contains("IBM");
	private static final String KERBEROS_PROP_PATH = "kerberos.properties";
	private static final Properties KERBEROS_PROPS = new Properties();
	// kerberos configs
	private static final String JAAS_CONF = "jaas.conf";
	private static final String KRB5_CONF = "krb5.conf";
	private static final String HADOOP_AUTH = "hadoop.security.authentication";
	private static final String SSL_CLIENT = "ssl-client.xml";
	private static final String HUAWEI_KEYTAB = "user.keytab";
	private static final String IBM_LOGIN_MODULE = "com.ibm.security.auth.module.Krb5LoginModule required";
	private static final String SUN_LOGIN_MODULE = "com.sun.security.auth.module.Krb5LoginModule required";
	// * java security login file path
	public static final String JAVA_SECURITY_LOGIN_CONF_KEY = "java.security.auth.login.config";
	private static final String JAVA_SECURITY_KRB5_CONF_KEY = "java.security.krb5.conf";
	private static final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";
	// keytab文件密码hash生成与用户输入的密码不匹配,通过kinit命令去校验检查
	private static final String LOGIN_FAILED_CAUSE_PASSWORD_WRONG = "(wrong password) keytab file and user not match, you can kinit -k -t keytab user in client server to check";
	private static final String LOGIN_FAILED_CAUSE_TIME_WRONG = "(clock skew) time of local server and remote server not match, please check ntp to remote server";
	private static final String LOGIN_FAILED_CAUSE_AES256_WRONG = "(aes256 not support) aes256 not support by default jdk/jre, need copy local_policy.jar and US_export_policy.jar from remote server in path /opt/huawei/Bigdata/jdk/jre/lib/security";
	private static final String LOGIN_FAILED_CAUSE_PRINCIPAL_WRONG = "(no rule) principal format not support by default, need add property hadoop.security.auth_to_local(in core-site.xml) value RULE:[1:$1] RULE:[2:$1]";
	private static final String LOGIN_FAILED_CAUSE_TIME_OUT = "(time out) can not connect to kdc server or there is fire wall in the network";

	public enum Module {
		STORM("StormClient"), KAFKA("KafkaClient"), ZOOKEEPER("Client");
		private String name;

		private Module(String name) {
			this.name = name;
		}

		public String getName() { return name; }
	}

	public static void kerberosAuth(Configuration conf, String kerberosPath) {
		String kerberosConfigPath = kerberosPath != null ? kerberosPath : Configs.gets("albatis.hbase.kerberos.path");
		if (null == kerberosConfigPath) return;
		try {
			KERBEROS_PROPS.load(IOs.openFile(kerberosConfigPath + KERBEROS_PROP_PATH));
		} catch (IOException e) {
			throw new RuntimeException("load KERBEROS_PROP error!", e);
		}
		String jaasFile = kerberosConfigPath + JAAS_CONF;
		String krb5ConfPath = kerberosConfigPath + KRB5_CONF;
		String keytabPath = kerberosConfigPath + KERBEROS_PROPS.getProperty("kerberos.keytab");
		String userPrincipal = KERBEROS_PROPS.getProperty("albatis.hbase.kerberos.hbase.principal") != null ? KERBEROS_PROPS.getProperty("albatis.hbase.kerberos.hbase.principal") : null;// user
		String zkServerPrincipal = KERBEROS_PROPS.getProperty("albatis.hbase.kerberos.zk.principal") != null ? KERBEROS_PROPS.getProperty("albatis.hbase.kerberos.zk.principal") : null;
		try {
			if (null != zkServerPrincipal)
				Kerberoses.setZookeeperServerPrincipal(zkServerPrincipal);
			if (Colls.list(new File(kerberosConfigPath).list()).contains(HUAWEI_KEYTAB)) {
				logger.info("Enable huawei kerberos!");
				Kerberoses.setJaasFile(userPrincipal, keytabPath);
				Kerberoses.login(userPrincipal, keytabPath, krb5ConfPath, conf);
			} else if (null == KERBEROS_PROPS.getProperty("albatis.hbase.kerberos.jaas.enable") || !Boolean.parseBoolean(KERBEROS_PROPS.getProperty("albatis.hbase.kerberos.jaas.enable"))) {
				logger.info("Enable normal kerberos without jaas file path!");
				Kerberoses.setKrb5Config(krb5ConfPath);
				conf.addResource(new Path(kerberosConfigPath + SSL_CLIENT));
				conf.set(HADOOP_AUTH, KERBEROS_PROPS.getProperty(HADOOP_AUTH, "Kerberos"));
				UserGroupInformation.setConfiguration(conf);
				UserGroupInformation.loginUserFromKeytab(KERBEROS_PROPS.getProperty("hbase.user"), keytabPath);
			} else {
				logger.info("Enable normal kerberos!");
				Kerberoses.setKrb5Config(krb5ConfPath);
				System.setProperty("java.security.auth.login.config", jaasFile);
				conf.addResource(new Path(kerberosConfigPath + SSL_CLIENT));
				conf.set(HADOOP_AUTH, KERBEROS_PROPS.getProperty(HADOOP_AUTH, "Kerberos"));
				UserGroupInformation.setConfiguration(conf);
				UserGroupInformation.loginUserFromKeytab(KERBEROS_PROPS.getProperty("hbase.user"), keytabPath);
			}
		} catch (IOException e) {
			logger.error("Loading kerberos properties is failure", e);
		}
	}

	private synchronized static void login(String userPrincipal, String userKeytabPath, String krb5ConfPath, Configuration conf)
			throws IOException {
		// 1.check input parameters
		if ((userPrincipal == null) || (userPrincipal.length() <= 0)) throw new IOException("input userPrincipal is invalid.");
		if ((userKeytabPath == null) || (userKeytabPath.length() <= 0)) throw new IOException("input userKeytabPath is invalid.");
		if ((krb5ConfPath == null) || (krb5ConfPath.length() <= 0)) throw new IOException("input krb5ConfPath is invalid.");
		if ((conf == null)) throw new IOException("input conf is invalid.");

		// 2.check file exsits
		File userKeytabFile = new File(userKeytabPath);
		if (!userKeytabFile.exists()) throw new IOException("userKeytabFile(" + userKeytabFile.getAbsolutePath() + ") does not exsit.");
		if (!userKeytabFile.isFile()) throw new IOException("userKeytabFile(" + userKeytabFile.getAbsolutePath() + ") is not a file.");

		File krb5ConfFile = new File(krb5ConfPath);
		if (!krb5ConfFile.exists()) throw new IOException("krb5ConfFile(" + krb5ConfFile.getAbsolutePath() + ") does not exsit.");
		if (!krb5ConfFile.isFile()) throw new IOException("krb5ConfFile(" + krb5ConfFile.getAbsolutePath() + ") is not a file.");

		// 3.set and check krb5config
		setKrb5Config(krb5ConfFile.getAbsolutePath());
		UserGroupInformation.setConfiguration(conf);

		// 4.login and check for hadoop
		loginHadoop(userPrincipal, userKeytabFile.getAbsolutePath());
		logger.info("Login success!!!!!!!!!!!!!!");
	}

	static boolean checkNeedLogin(String principal) throws IOException {
		if (!UserGroupInformation.isSecurityEnabled()) throw new IOException(
				"UserGroupInformation is not SecurityEnabled, please check if core-site.xml exists in classpath.");
		UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
		if ((currentUser == null) || (!currentUser.hasKerberosCredentials())) return true;
		if (checkCurrentUserCorrect(principal)) {
			logger.info("current user is " + currentUser + "has logined.");
			if (currentUser.isFromKeytab()) return false;
			throw new IOException("current user is not from keytab.");
		} else throw new IOException("current user is " + currentUser
				+ " has logined. And please check your enviroment!! especially when it used IBM JDK or kerberos for OS count login!!");
	}

	static void setKrb5Config(String krb5ConfFile) throws IOException {
		System.setProperty(JAVA_SECURITY_KRB5_CONF_KEY, krb5ConfFile);
		String ret = System.getProperty(JAVA_SECURITY_KRB5_CONF_KEY);
		if (ret == null) throw new IOException(JAVA_SECURITY_KRB5_CONF_KEY + " is null.");
		if (!ret.equals(krb5ConfFile)) throw new IOException(JAVA_SECURITY_KRB5_CONF_KEY + " is " + ret + " is not " + krb5ConfFile + ".");
	}

	static void setJaasFile(String principal, String keytabPath) throws IOException {
		String jaasPath = new File(System.getProperty("java.io.tmpdir")).toString() + File.separatorChar + System.getProperty("user.name") + '.'
				+ JAAS_CONF;

		// windows路径下分隔符替换
		jaasPath = jaasPath.replace("\\", "\\\\");
		keytabPath = keytabPath.replace("\\", "\\\\");
		// 删除jaas文件
		deleteJaasFile(jaasPath);
		writeJaasFile(jaasPath, principal, keytabPath);
		System.setProperty(JAVA_SECURITY_LOGIN_CONF_KEY, jaasPath);
	}

	private static void writeJaasFile(String jaasPath, String principal, String keytabPath) throws IOException {
		try (FileWriter writer = new FileWriter(new File(jaasPath));) {
			writer.write(getJaasConfContext(principal, keytabPath));
			writer.flush();
		} catch (IOException e) {
			throw new IOException("Failed to create jaas.conf File");
		}
	}

	private static void deleteJaasFile(String jaasPath) throws IOException {
		File jaasFile = new File(jaasPath);
		if (jaasFile.exists() && !jaasFile.delete()) throw new IOException("Failed to delete exists jaas file.");
	}

	private static String getJaasConfContext(String principal, String keytabPath) {
		Module[] allModule = Module.values();
		StringBuilder builder = new StringBuilder();
		for (Module modlue : allModule)
			builder.append(getModuleContext(principal, keytabPath, modlue));
		return builder.toString();
	}

	private static String getModuleContext(String userPrincipal, String keyTabPath, Module module) {
		StringBuilder builder = new StringBuilder(module.getName()).append(" {").append(LINE_SEPARATOR);
		if (IS_IBM_JDK) {
			builder.append(IBM_LOGIN_MODULE).append(LINE_SEPARATOR)//
					.append("credsType=both").append(LINE_SEPARATOR)//
					.append("principal=\"" + userPrincipal + "\"").append(LINE_SEPARATOR)//
					.append("useKeytab=\"" + keyTabPath + "\"").append(LINE_SEPARATOR)//
					.append("debug=true;").append(LINE_SEPARATOR)//
					.append("};").append(LINE_SEPARATOR);
		} else {
			builder.append(SUN_LOGIN_MODULE).append(LINE_SEPARATOR)//
					.append("useKeyTab=true").append(LINE_SEPARATOR)//
					.append("keyTab=\"" + keyTabPath + "\"").append(LINE_SEPARATOR)//
					.append("principal=\"" + userPrincipal + "\"").append(LINE_SEPARATOR)//
					.append("useTicketCache=false").append(LINE_SEPARATOR)//
					.append("storeKey=true").append(LINE_SEPARATOR)//
					.append("debug=true;").append(LINE_SEPARATOR)//
					.append("};").append(LINE_SEPARATOR);
		}

		return builder.toString();
	}

	static void setJaasConf(String loginContextName, String principal, String keytabFile) throws IOException {
		if ((loginContextName == null) || (loginContextName.length() <= 0)) throw new IOException("input loginContextName is invalid.");
		if ((principal == null) || (principal.length() <= 0)) throw new IOException("input principal is invalid.");
		if ((keytabFile == null) || (keytabFile.length() <= 0)) throw new IOException("input keytabFile is invalid.");
		File userKeytabFile = new File(keytabFile);
		if (!userKeytabFile.exists()) throw new IOException("userKeytabFile(" + userKeytabFile.getAbsolutePath() + ") does not exsit.");

		javax.security.auth.login.Configuration.setConfiguration(//
				new JaasConfiguration(loginContextName, principal, userKeytabFile.getAbsolutePath()));

		javax.security.auth.login.Configuration conf = javax.security.auth.login.Configuration.getConfiguration();
		if (!(conf instanceof JaasConfiguration)) throw new IOException("javax.security.auth.login.Configuration is not JaasConfiguration.");

		AppConfigurationEntry[] entrys = conf.getAppConfigurationEntry(loginContextName);
		if (entrys == null) throw new IOException("javax.security.auth.login.Configuration has no AppConfigurationEntry named "
				+ loginContextName + ".");

		boolean checkPrincipal = false;
		boolean checkKeytab = false;
		for (int i = 0; i < entrys.length; i++) {
			if (entrys[i].getOptions().get("principal").equals(principal)) checkPrincipal = true;
			if (IS_IBM_JDK) {
				if (entrys[i].getOptions().get("useKeytab").equals(keytabFile)) checkKeytab = true;
			} else {
				if (entrys[i].getOptions().get("keyTab").equals(keytabFile)) checkKeytab = true;
			}
		}
		String info = "AppConfigurationEntry named " + loginContextName + " does not have ";
		if (!checkPrincipal) throw new IOException(info + "principal value of " + principal + ".");
		if (!checkKeytab) throw new IOException(info + "keyTab value of " + keytabFile + ".");
	}

	public static void setZookeeperServerPrincipal(String zkServerPrincipal) throws IOException {
		System.setProperty(ZOOKEEPER_SERVER_PRINCIPAL_KEY, zkServerPrincipal);
		String ret = System.getProperty(ZOOKEEPER_SERVER_PRINCIPAL_KEY);
		if (ret == null) throw new IOException(ZOOKEEPER_SERVER_PRINCIPAL_KEY + " is null.");
		if (!ret.equals(zkServerPrincipal)) throw new IOException(ZOOKEEPER_SERVER_PRINCIPAL_KEY + " is " + ret + " is not " + zkServerPrincipal
				+ ".");

	}

	@Deprecated
	public static void setZookeeperServerPrincipal(String zkServerPrincipalKey, String zkServerPrincipal) throws IOException {
		System.setProperty(zkServerPrincipalKey, zkServerPrincipal);
		String ret = System.getProperty(zkServerPrincipalKey);
		if (ret == null) throw new IOException(zkServerPrincipalKey + " is null.");
		if (!ret.equals(zkServerPrincipal)) throw new IOException(zkServerPrincipalKey + " is " + ret + " is not " + zkServerPrincipal + ".");
	}

	private static void loginHadoop(String principal, String keytabFile) throws IOException {
		try {
			UserGroupInformation.loginUserFromKeytab(principal, keytabFile);
		} catch (IOException e) {
			logger.error(new StringBuilder("login failed with ").append(principal).append(" and ").append(keytabFile)//
					.append("\n\tperhaps cause 1 is ").append(LOGIN_FAILED_CAUSE_PASSWORD_WRONG)//
					.append("\n\tperhaps cause 2 is ").append(LOGIN_FAILED_CAUSE_TIME_WRONG)//
					.append("\n\tperhaps cause 3 is ").append(LOGIN_FAILED_CAUSE_AES256_WRONG)//
					.append("\n\tperhaps cause 4 is ").append(LOGIN_FAILED_CAUSE_PRINCIPAL_WRONG)//
					.append("\n\tperhaps cause 5 is ").append(LOGIN_FAILED_CAUSE_TIME_OUT).toString(), e);
			throw e;
		}
	}

	public static void checkAuthenticateOverKrb() throws IOException {
		UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
		UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
		if (loginUser == null) throw new IOException("current user is " + currentUser + ", but loginUser is null.");
		if (!loginUser.equals(currentUser)) throw new IOException("current user is " + currentUser + ", but loginUser is " + loginUser + ".");
		if (!loginUser.hasKerberosCredentials()) throw new IOException("current user is " + currentUser + " has no Kerberos Credentials.");
		if (!UserGroupInformation.isLoginKeytabBased()) throw new IOException("current user is " + currentUser + " is not Login Keytab Based.");
	}

	private static boolean checkCurrentUserCorrect(String principal) throws IOException {
		UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
		if (ugi == null) throw new IOException("current user still null.");

		String defaultRealm = null;
		try {
			defaultRealm = org.apache.hadoop.security.authentication.util.KerberosUtil.getDefaultRealm();
		} catch (Exception e) {
			throw new IOException("getDefaultRealm failed.", e);
		}

		if ((defaultRealm != null) && (defaultRealm.length() > 0)) {
			StringBuilder realm = new StringBuilder();
			StringBuilder principalWithRealm = new StringBuilder();
			realm.append("@").append(defaultRealm);
			if (!principal.endsWith(realm.toString())) {
				principalWithRealm.append(principal).append(realm);
				principal = principalWithRealm.toString();
			}
		}

		return principal.equals(ugi.getUserName());
	}

	// * copy from hbase zkutil 0.94&0.98 A JAAS configuration that defines the login modules that we want to use for login.
	private static class JaasConfiguration extends javax.security.auth.login.Configuration {
		private static final Map<String, String> BASIC_JAAS_OPTIONS = new HashMap<String, String>();
		static {
			String jaasEnvVar = System.getenv("HBASE_JAAS_DEBUG");
			if (jaasEnvVar != null && "true".equalsIgnoreCase(jaasEnvVar)) BASIC_JAAS_OPTIONS.put("debug", "true");
		}
		private static final Map<String, String> KEYTAB_KERBEROS_OPTIONS = new HashMap<String, String>();
		static {
			if (IS_IBM_JDK) KEYTAB_KERBEROS_OPTIONS.put("credsType", "both");
			else {
				KEYTAB_KERBEROS_OPTIONS.put("useKeyTab", "true");
				KEYTAB_KERBEROS_OPTIONS.put("useTicketCache", "false");
				KEYTAB_KERBEROS_OPTIONS.put("doNotPrompt", "true");
				KEYTAB_KERBEROS_OPTIONS.put("storeKey", "true");
			}

			KEYTAB_KERBEROS_OPTIONS.putAll(BASIC_JAAS_OPTIONS);
		}
		private static final AppConfigurationEntry KEYTAB_KERBEROS_LOGIN = new AppConfigurationEntry(
				org.apache.hadoop.security.authentication.util.KerberosUtil.getKrb5LoginModuleName(), LoginModuleControlFlag.REQUIRED,
				KEYTAB_KERBEROS_OPTIONS);
		private static final AppConfigurationEntry[] KEYTAB_KERBEROS_CONF = new AppConfigurationEntry[] { KEYTAB_KERBEROS_LOGIN };
		private javax.security.auth.login.Configuration baseConfig;
		private final String loginContextName;
		private final boolean useTicketCache;
		private final String keytabFile;
		private final String principal;

		public JaasConfiguration(String loginContextName, String principal, String keytabFile) throws IOException {
			this(loginContextName, principal, keytabFile, keytabFile == null || keytabFile.length() == 0);
		}

		private JaasConfiguration(String loginContextName, String principal, String keytabFile, boolean useTicketCache) throws IOException {
			try {
				this.baseConfig = javax.security.auth.login.Configuration.getConfiguration();
			} catch (SecurityException e) {
				this.baseConfig = null;
			}
			this.loginContextName = loginContextName;
			this.useTicketCache = useTicketCache;
			this.keytabFile = keytabFile;
			this.principal = principal;

			initKerberosOption();
			logger.info("JaasConfiguration loginContextName=" + loginContextName + " principal=" + principal + " useTicketCache="
					+ useTicketCache + " keytabFile=" + keytabFile);
		}

		private void initKerberosOption() throws IOException {
			if (!useTicketCache) {
				if (IS_IBM_JDK) KEYTAB_KERBEROS_OPTIONS.put("useKeytab", keytabFile);
				else {
					KEYTAB_KERBEROS_OPTIONS.put("keyTab", keytabFile);
					KEYTAB_KERBEROS_OPTIONS.put("useKeyTab", "true");
					KEYTAB_KERBEROS_OPTIONS.put("useTicketCache", useTicketCache ? "true" : "false");
				}
			}
			KEYTAB_KERBEROS_OPTIONS.put("principal", principal);
		}

		@Override
		public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
			if (loginContextName.equals(appName)) return KEYTAB_KERBEROS_CONF;
			if (baseConfig != null) return baseConfig.getAppConfigurationEntry(appName);
			return null;
		}
	}
}
