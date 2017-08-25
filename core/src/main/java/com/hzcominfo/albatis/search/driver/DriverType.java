/*
 * 文件名：DriveType.java
 * 版权： 
 * 描述:支持数据源的一个枚举
 * 创建人： 郎敬翔
 * 修改时间：2016-11-15
 * 操作：创建
 */
package com.hzcominfo.albatis.search.driver;

/**
 * 枚举数据库的类型,记录枚举数据库名称和驱动调用的类名支持的驱动类型
 *
 * @author ljx
 * @version 0.0.1
 * @see
 */
public enum DriverType {
	ESDB("elasticsearch", "com.hzcominfo.albatis.search.es.core.drive.ElasticDriver", "com.hzcominfo.es.description", "Es"), SOLR("solr",
			"com.hzcominfo.albatis.search.common.solr.core.drive.solr", "com.hzcominfo.es.description", "Es");

	private String name;
	private String drive;
	private String descriptionPackage;
	private String prefix;

	DriverType(String name, String drive, String descriptionPackage, String prefix) {
		this.name = name;
		this.drive = drive;
		this.descriptionPackage = descriptionPackage;
		this.prefix = prefix;
	}

	public String getName() {
		return name;
	}

	public String getDrive() {
		return drive;
	}

	public String getDescriptionPackage() {
		return descriptionPackage;
	}

	public String getPrefix() {
		return prefix;
	}
}
