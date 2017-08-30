## 通用搜索说明
### albatis-es
1. mvn依赖：
	~~~xml
		<dependency>
			<groupId>net.butfly.albatis</groupId>
			<artifactId>albatis-es</artifactId>
			<version>1.2.0-SNAPSHOT</version>
		</dependency>
	~~~
	如果需要设置数据权限：
	~~~xml
		<dependency>
            <groupId>com.hzcominfo.dataggr</groupId>
            <artifactId>dataggr-authority</artifactId>
            <version>1.0.0-SNAPSHOT</version>
        </dependency>
	~~~
2. 调用示例
	1. connection create
		~~~java
		Connection connection = DriverManager.getConnection("elasticsearch://phgacluster@10.120.173.60:39300");
		~~~
	1. 将sql解析成es查询语句 （不支持多表及别名）
		~~~java
			PreparedStatementBase pstat = (PreparedStatementBase) connection.prepareStatement(sql);
		~~~
		1. sql结构:
			~~~sql
				select * from indexName..typeName where [condition] 
			~~~
		2. sql示例：
			~~~sql
				select * from indexName..typeName where zjhm='574646121111111' and name like '.*tom.*'; 
			~~~
	2. 设置分片size，默认值为10L，仅当使用aggregation时使用
		~~~java
			pstat.setShardSize(10L);
		~~~
	3. 设置当前查询用户的权限
		~~~java
			pstat.setAuthKey("authKey");
		~~~
	4. 设置结果集中的过滤参数
		~~~java
			pstat.setFilterParam("filterParam");
		~~~
	5. 执行es查询
		~~~java
			ResultSetBase rbase = (ResultSetBase) pstat.executeQuery();			
		~~~
	6. 返回结果
		~~~java
		List<Map<String, Object>> resultMapList = rbase.getResultMapList();		
		//可以选择是否去掉map中key包含的数据库名和表名，只留字段名
		//List<Map<String, Object>> resultMapList = rbase.onlyFieldResult(rbase.getResultMapList());
		~~~
1. GEO搜索说明
	1. 各类形状定义函数
		1. 矩形 geoBoundingBoxQuery(field, top, left, bottom, right)
		1. 圆形 geoDistanceQuery(field, lat, lon, distance) 
		1. 多边形 geoPolygonQuery(field, lat%lon,lat%lon,lat%lon ...)
	1. 查询调用结构 (GEO函数一定要放在末尾)
		~~~sql
			select * from indexName..typeName where [condition] and geoBoundingBoxQuery(location, 3.0, 2.0, 1.5, 4.0)
		~~~
	1. 查询调用示例
		~~~sql
			select * from hzwa_dev_20170822..NEW_WA_SOURCE_FJ_1001 where geoBoundingBoxQuery(LOCATION, 30.1860760000000, 120.2812200000000, 30.1528250000000, 120.2975510000000) limit 100
		~~~	
1. ES地址
http://10.118.159.44:39201/

1.权限
	如需过滤权限，请阅读dataggr/dataggr-filter/README.MD
	
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
