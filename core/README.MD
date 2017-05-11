## 通用搜索说明
### albatis-es
1. mvn依赖：
	~~~xml
	<dependency>
		<groupId>net.butfly.albatis</groupId>
		<artifactId>albatis-es</artifactId>
		<version>1.0.1-SNAPSHOT</version>
	</dependency>
	~~~
2. 调用示例
	1. 将sql解析成es查询语句 
		~~~java
			ElasticPrepareStatement pstat = connection.prepareStatement(sql);
		~~~
		1. sql结构:
			~~~sql
			select * from index..type where [condition] 
			~~~
	2.设置分片size，默认值为10L，仅当使用aggregation时使用
		~~~java
			pstat.setShardSize(10L);
		~~~
	3.执行es查询
		~~~java
			ResultSet rs = pstat.executeQuery();		
		~~~
	2.返回结果
		~~~java
			ElasticResultMetadata searchResult = (ElasticResultMetadata) rs.getMetaData();
		~~~
3. ES地址
http://10.118.159.44:39201/
	
### albatis-solr    
目前没有用到，而且里边的类都为@Deprecated

    
## Query

## Result

## Filter设计文档

### 功能需求
目前需要在数据的处理中增加权限管理和数据过滤操作。其定义如下：
- 权限管理：对于一些请求，按照给定的附加参数，如用户、用户组等。
结合配置表，应可以对数据集合的可见性以及对数剧列的可见性有一定的限制
- 数据过滤：根据定义的规则，一些数据应该进行值的覆盖，以隐藏其真实的数据值。
 
扩展性要求：以上的功能，仅仅是就目前一直的业务，但是后期业务类型增多的情况下，
应该尽可能的的减少编码工作，并且减少服务重新打包和部署的次数。

### 设计概述
总体上，使用责任链模式，所有插入的项目过滤器应实现Filter接口，并通过FilterChain来进行
管理和调用。FilterChain内部挂载的Filter通过XML文件可以配置，XML文件格式固定。

对于驱动的开发者，主要基于本项目(albatis-core)，确定FlterChain的使用位置以及对FilterChain
进行调用。

对于Filter的开发者，也即具体业务的实现者，则主要完成对Filter接口的实现，基于接口编码参数和返回规约
进行实际的业务开发。

对于整个Driver的调用方，如具体服务的开发者，只需要进行XML文件的配置和引入正确的jar包。
既可以在开发中使用，整体来说，除了应有的异常返回和日志记录以外，整个过滤对于业务流程是透明的。

### 类型职责
Filter：具体的执行者，实现定义的接口，完成对输入的处理并返回结果。必须有无参构造方法

FilterChain：Filter的管理对象，以及系统内直接调用对象。
通过配置文件配置，在调用时通过Java反射机制生成并返回。
其中管理了Filter的调用顺序，Filter参数检验(在生成FilterChain时，检验注入的Filter能否满足Chain的完备性)

FilterLoader：FilterChain的工具类，同时也是使用工厂方法获取filterChain的入口
### 配置样例
#### 样例
```xml
<beans>
    <filterChain name="queryFilterChain1" class="com.hzcominfo.albatis.search.filter.BasicFilterChain"
                invoke="com.hzcominfo.albatis.search.filter.Sample">
        <filter class="com.hzcominfo.albatis.search.filter.LogFilter"
                order="3">
            <param key="paramA" value="valueA"/>
        </filter>
        <param key="paramA" value="valueA"/>
    </filterChain>

    <filterChain name="queryFilterChain2" class="com.hzcominfo.albatis.search.filter.BasicFilterChain">
        <filter class="com.hzcominfo.albatis.search.filter.ResultFilter">
            <param key="paramA" value="valueA"/>
        </filter>
        
    </filterChain>
</beans>
```
#### 配置解释
以上的配置层级必须直接在一个xml文件中。

* filterChain部分：name用于按需构造，定义多个filterChain，可以在XML文件中指出，
驱动层处理请求附带的特殊参数并将此参数传递给连接对象(如Connection)，从而配置一个指定的FilterChain
    - name   (not null) 配置内必须唯一，如果检测到配置name冲突，则打印警告信息并保留首次扫描到的配置。
    - invoke (nullable) 限定使用的Class，这里主要是为了防止在切换不同的driver实现时，保留了错误的配置。
    - class  (not null) FilterChain的实现类 如不指定则使用系统默认的实现
    - param  (nullable) 用于驱动层构造FilterChain时指定对象内属性的值。只能使用Java内的基本数据类型(以及包装类)
    - filter 详见filter部分
* filter部分：定义具体的Filter
    - class  (not null) Filter类 如果指定的Filter类不存在，则相应的FilterChain无法构建。
    - order  (not null) Filter顺序 如果排序值冲突，则相应filterChain无法构建。
    - param  (nullable) 用于驱动层构造FilterChain时指定对象内属性的值。只能使用Java内的基本数据类型(以包封装类)

### 处理逻辑
在通过工厂方法获取FilterChain的时候，工厂方法(invoke)支持两种参数
- 传入name值，此时使用传入的name并查找配置表，按照给定的配置生成FilterChain或者抛出异常
- 不传入参数，此时搜索配置文件中第一个invoke项存在并且invoke值为当前调用类的filterChain
构造并返回，或者抛出异常
