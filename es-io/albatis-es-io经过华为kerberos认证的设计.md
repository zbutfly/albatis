#albatis-es-io经过华为kerberos认证的设计

为了调用华为的es服务需要通过华为的kerberos认证

## 参考资料
华为es kerberos 认证开发示例：http://support-it.huawei.com/solution-fid-gw/#/compCases55

## 涉及的相关配置文件

所有的配置文件均存放在同一指定目录下，约定文件名称，es kerberos认证需要包含如下文件：

1.kerberos 认证凭据文件：
     1)jaas.conf  存放es_server_jaas.conf文件（常规kerberos需提供）
     2)krb5.conf

2. albatis-es.properties

java.security.krb5.conf = `es krb5文件路径`
java.security.auth.login.config = `jaas 文件路径`
javax.security.auth.useSubjectCredsOnly = false
es.security.indication = true
