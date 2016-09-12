# ALBATIS-KAFKA 使用手册
## 配置
1. 框架配置
在业务项目的spring配置文件中，包含albatis-kafka主定义文件：
```xml
<import resource="classpath:/net/butfly/albatis/kafka/spring/beans-kafka.xml" />
```
2. 业务配置
在业务项目的spring配置文件中，定义需读取的kafka topic：
```xml
<bean id="kafkaTopicConfig1" class="net.butfly.albatis.impl.kafka.mapper.KafkaTopicConfig">
	<property name="topic" value="${test.topic1.topic}" />
	<property name="streamNum" value="${test.topic1.stream.num}" />
	<property name="key" value="${test.topic1.key}" />
</bean>
```
3. 参数配置
定义classpath:/kafka-config.properties：
	```
	albatis.kafka.zookeeper.connect=
	albatis.kafka.group.id=

	### optional parameters, default:
	#albatis.kafka.zookeeper.connection.timeout.ms=15000
	#albatis.kafka.zookeeper.sync.time.ms=5000
	#albatis.kafka.auto.commit.enable=false
	#albatis.kafka.auto.commit.interval.ms=1000
	#albatis.kafka.auto.offset.reset=smallest
	#albatis.kafka.session.timeout.ms=30000
	#albatis.kafka.partition.assignment.strategy=range
	## 5*1024*1024
	#albatis.kafka.socket.receive.buffer.bytes=5242880
	## 3*1024*1024
	#albatis.kafka.fetch.message.max.bytes=3145728

	test.buffer.mix=
	test.msg.buffers=
	test.max.buffer=

	## topics parameters
	test.topic1.topic=
	test.topic1.stream.num=
	test.topic1.key=
	```