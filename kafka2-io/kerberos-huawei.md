# albatis-kafka-io经过华为的kerberos认证的设计

为了调用华为集群的kafka，需要通过华为的kerberos认证。

## 参考资料

根据华为提供的文档，请看
[华为二次开发环境准备](https://forum.huawei.com/enterprise/zh/thread-471209.html)

## 设计

### 配置文件

所有配置文件均保存在一个配置目录中，使用约定的文件名。

应包含以下配置文件：

1. kerberos证书文件

	1. jaas.conf

		存放kafka_server_jaas.conf文件（常规kerberos需提供）

	2. krb5.conf

		存放krb5.conf。

3. huawei.keytab
		
		华为kerberos证书文件。

2. kerberos.properties
	
	存放kafka principal和zk principal
	- albatis.kafka.kerberos.kafka.principal=`kafka的principal值` // 必须提供，不可为空
	- albatis.kafka.kerberos.zk.principal=`zk的principal值` // 必须提供，不可为空
	- kerberos.domain.name=`kerberos.domain的值` // 必须提供，不可为空
	- security.protocol=`SASL_PLAINTEXT` // 可不提供，默认值为SASL_PLAINTEXT
	- sasl.kerberos.service.name=`kafka`  // 可不提供，默认值为kafka


这些配置文件所在的文件夹路径可以由配置项配入

	albatis.kafka.kerberos=配置文件夹路径  //常规kerberos的配置路径

如果不配置，则不走kerberos认证。

如果配置目录中有huawei.keytab，则自动初始化华为Kerberos认证，否则为标准Kerberos配置。

### URI参数配置

暂时不支持url参数配置，有待后续实现。

