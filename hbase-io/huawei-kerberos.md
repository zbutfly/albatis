#albatis-hbase-io经过华为kerberos认证的设计

为了调用华为的hbase集群需要通过华为的kerberos认证

## 参考资料
华为hbase kerberos 认证开发示例：http://support-it.huawei.com/solution-fid-gw/#/compCases3

## 涉及的相关配置文件

所有的配置文件均存放在同一指定目录下，约定文件名称，hbase kerberos认证需要包含如下文件：

1.kerberos 认证凭据文件：
   1)jaas.conf   存放hbase_server_jaas.conf文件（常规kerberos需提供）
   2)krb5.conf
   3)user.keytab   华为用户管理界面生成的用户认证信息文件

2.albatis-hbase.properties
 - albatis.hbase.kerberos.hbase.principal=`hbase的principal值` // 必须提供，不可为空
 - albatis.hbase.kerberos.zk.principal=`zk的principal值`  // 必须提供，不可为空
 - hbase.security.authentication = kerberos


这些配置文件所在的文件夹路径可以由配置项配入

	-albatis.hbase.kerberos.path=配置文件夹路径  //常规kerberos的配置路径

如果不配置，则不走kerberos认证。

如果配置目录中有user.keytab，则自动初始化华为Kerberos认证，否则为标准Kerberos配置。



