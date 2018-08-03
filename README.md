# 统一查询
## 使用说明

#### 本地调用 （以solr为例）
1. mvn 依赖
	1. solr
		~~~xml
			<dependency>
				<groupId>com.hzcominfo.dataggr.uniquery</groupId>
				<artifactId>uniquery-solr</artifactId>
				<version>0.0.2-SNAPSHOT</version>
			</dependency>
		~~~
	
	1. es5
		~~~xml
			<dependency>
				<groupId>com.hzcominfo.dataggr.uniquery</groupId>
				<artifactId>uniquery-es5</artifactId>
				<version>0.0.2-SNAPSHOT</version>
			</dependency>
		~~~

1. 本地调用代码实例
	1. 根据url串创建URISpec实例
		1. solr
			~~~java
				String uri = "solr:http://cdh001:10180/solr";
				//String uri = "zk:solr://cdh001:12181,cdh002:12181,cdh003:12181";
			~~~
		1. es5 
			~~~java
				String uri = "es://pseudo-elasticsearch@172.16.27.232:9300";
				//String uri = "elasticsearch://pseudo-elasticsearch@172.16.27.232:9300";
			~~~
		1. mongo
			~~~java
				String uri = "mongodb://base:base1234@172.16.17.11:40012/basedb";
			~~~	
		~~~java
			URISpec uriSpec = new URISpec(uri);
		~~~
		
	1. 创建统一搜索客户端
		~~~java
			Client client = new Client(uriSpec);
		~~~
		
	1. 执行sql并得到结果
		1. common query
			1. solr, mongo 
				~~~java
					String sql = "select * from tdl_c1 where id=? and XB_FORMAT_s=?";
					Object[] params = {"593fb635c00282f4af41bdd0","男"}; //动态参数
				~~~
			
			1. es
				~~~java
					String sql = "select * from uniquery.sell";
					Object[] params = {};
				~~~
			~~~java
				ResultSet rs = client.execute(sql, params);	
				List<Map<String, Object>> results = rs.getResults();
				long total = rs.getTotal();			
			~~~
		
		1. group by 
			~~~java
				String sql = "select XB_FORMAT_s,MZ_FORMAT_s from tdl_c1 group by XB_FORMAT_s,MZ_FORMAT_s";
				Object[] params = {};
				ResultSet rs = client.execute(sql, params);
				List<Map<String, Object>> results = rs.getResults();			
			~~~
			
		1. multi group by
			~~~java
				String sql = "select * from tdl_c1";
				String[] facets = {"XB_FORMAT_s", "MZ_FORMAT_s", "MZ_i"}; //facet字段 一个数组元素可包含多个facet字段,用  , 隔开
				Object[] params = {};
				Map<String, ResultSet> results = conn.execute(sql, facets, params);	
			~~~
		
		1. batch operation
			~~~java
				Request request = new Request();
				request.setKey("0");
				request.setSql("select * from tdl_c1");
				Object[] params = {};
				request.setParams(params);
				
				Request request1 = new Request();
				request1.setKey("1");
				request1.setSql("select * from tdl_c2");
				Object[] params1 = {};
				request1.setParams(params1);
				
				Map<String, ResultSet> results = conn.batchExecute(request, request1);
			~~~
		1. geo func search
			~~~java
				String sql = "select geo_p from uniquery_solr_test where geo_distance(geo_p,33.00,22.00,5)"; // 圆形
				//sql = "select geo_p from uniquery_solr_test where geo_box(geo_p,44.00,11.00,11.00,44.00)"; // 矩形
				//sql = "select geo_p from uniquery_solr_test where geo_polygon(geo_srpt,-10,30, -40,40, -10,-20,40,0, 40,30, -10,30)"; // 多边形
				ResultSet rs = conn.execute(sql, "");
				List<Map<String, Object>> result = rs.getResults();
				System.out.println(result);
			~~~ 
	
	1. 关闭客户端
		~~~java
			client.close();
		~~~
		
1. SQL功能支持
	1. 只支持查询语句，不支持插入、更新、修改、删除等操作
	
	2. 暂不支持函数
	
	3. 字段支持情况：
	    1. *, field_a, field_b[, field_c ...]
	    
	4. 表支持情况：
	    1. 能解析多表，但暂时只支持单表操作。
	    
	5. 过滤条件支持情况：
	    1. 不指定, AND, OR, IS NULL, IS NOT NULL, >, >=, <, <=, =, <>, LIKE, NOT LIKE, BETWEEN AND, NOT BETWEEN AND, IN, NOT IN.
	    
	6. 排序支持情况
	    1. ORDER BY field_a ASC/DESC[, field_b ASC, DESC ...]
	    
	7. 查询数量支持情况
	    1. 支持 LIMIT 关键字(可选，可通过系统属性'uniquery.default.limit'指定默认值, 若'uniquery.default.limit'未指定，则默认值为10000 )
	    2. 支持 OFFSET 关键字(可选，不填则默认为 0)
	    3. OFFSET 需要在 LIMIT 之后出现 (如果有)
	
	8. 动态参数支持情况
	    1. 字段列表、查询条件中支持动态参数(用 ? 占位), 在explain时需要把参数传入
	
	9. cache功能
	    1. 以HashMap方式简单支持 
	    
	1. 统计功能	
		已支持数据统计：count(*)	
		
	1. 空间搜索支持 
		- 支持空间搜索函数（field为坐标字段；lat为纬度，lon为经度；x为经度,y为纬度；d为圆半径——km；top bottom对应矩形 上 下纬度，left bottom对应矩形的 左右经度）：
			- 圆形  geo_distance(field,lat,lon,d)
			- 矩形  geo_box(field,top,left,bottom,right)
			- 多边形	  geo_polygon(field,x1,y1,x2,y2...xn,yn,x1,y1)
	
	1. 全文检索功能
		- 已支持solr,es5的全文检索：query_all('v')
		
	1. 聚合查询数量支持情况
	    1. 可通过系统属性'uniquery.default.aggsize'指定默认值, 若未指定，则默认值为10 
		