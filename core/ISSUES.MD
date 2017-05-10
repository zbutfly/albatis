#通用搜索存在问题
##albatis-es
1. group by
	es版本升级（2.x to 5.x）后，对字段field做group by 需要 写成field.keyword，否则会报错。
2. 缺少查询多表信息的功能
	执行该查询需返回指定index下符合条件的信息，不需要指定type
3. 嵌套查询
	能否实现嵌套查询
##albatis-solr
1. 目前没有用到，而且里边的类都为@Deprecated