/*
 * 文件名：SourceSQL.java
 * 版权： 
 * 描述
 * 创建人： 郎敬翔
 * 修改时间：2016-11-14
 * 操作：创建
 */

package com.hzcominfo.albatis.nosql.search;

/**
 * 直接注入源语句
 *
 * @author ljx
 * @version 0.0.1
 * @pdOid ad40dfbf-bc87-4b92-b646-d2d377952fac
 * @see
 */
@Deprecated
public interface SourceSQL extends Describe {
    /**
     * @param sourceSQL 源码
     * @pdOid 5a7031e3-844c-48e8-871e-abf0b3585226
     */
    void add(String sourceSQL);

    /**
     * @pdOid 2194aef2-9c28-4f70-a4be-5434fd3b1799
     */
    String getSourceSQL();

}