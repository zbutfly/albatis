## 通用搜索存在问题
### albatis-es
1. group by    
	问题：es版本升级（2.x to 5.x）后，对字段field做group by 需要 写成field.keyword，否则会报错。
	方案：在aggregation的方法中对这些字段做 + .keyword的拼接
	现状：已在版本albatis-es-1.0.1-SNAPSHOT中实现。
2. 缺少查询多表信息的功能    
	问题：执行该查询需返回指定index即多个type下符合条件的信息
	方案：目前是外部传一个sql语句，有albatis-es来解析成SearchRequestBuilder，而srBuilder的types本身支持多type，可以调整。
	现状：已在版本albatis-es-1.0.1-SNAPSHOT中实现。
3. 子查询    
	问题：能否实现子查询
	方案：可以通过配置mapping，同个index下定义文档（type）间的父子关系，用has_parent|has_child实现 。
		但是只能实现简单的关联，不满足所提要求。
	现状：无法支持。
4. aggregation分片大小问题
	问题：aggregation时分片设置为10造成部分数据统计错误
	方案：目前是在类ElasticQuery中定义了shard_size的数值10，可修改相关方法，使得外部可以修改shard_size的值。
	现状：已在版本albatis-es-1.0.1-SNAPSHOT中实现。
### albatis-solr
1. 目前没有用到，而且里边的类都为@Deprecated