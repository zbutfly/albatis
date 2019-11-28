package net.butfly.albatis.kudu;

import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.IOs;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.kerberos.KerberosLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;


public class Kerberos extends KerberosLoader {
    private static final Logger logger = Logger.getLogger(Kerberos.class);
    private final String KRB5_USER = Configs.gets("krb5.user", "krb5.user");
    private final String USER_TAB = Configs.gets("user.keytab", "user.keytab");
    private String user;

    public Kerberos(String kerberosConfPath) {
        super(kerberosConfPath);
        try {
            KERBEROS_PROPS.load(IOs.openFile(kerberosConfPath + KERBEROS_PROPERTIES));
            user = KERBEROS_PROPS.getProperty(KRB5_USER);
            Configuration configuration=new Configuration();
            configuration.set("hadoop.security.authentication", "Kerberos");
            System.setProperty("java.security.krb5.conf", new File(kerberosConfPath,KRB5_CONF).getPath());
            System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
            UserGroupInformation.setConfiguration(configuration);
            UserGroupInformation.loginUserFromKeytab(user, new File(kerberosConfPath,KERBEROS_PROPS.getProperty(USER_TAB)).getPath());
        } catch (Exception e) {
            logger.error("load jdbc kerberos config error:"+e);
        }
    }

    public String getUser() {
        return user;
    }
}
