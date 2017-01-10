/*
 * 文件名：Action.java
 * 版权： 
 * 描述:对数据的操作行为
 * 创建人： 郎敬翔
 * 修改时间：2016-11-14
 * 操作：创建
 */

package com.hzcominfo.albatis.nosql.search;

/**
 * 对应要操作的行为
 *
 * @author ljx
 * @version 0.0.1
 * @pdOid 6031ceee-9465-46d5-b7d0-4f3fa5456a71
 * @see
 */
public interface Action {
    /**
     * 要操作的行为
     *
     * @return ActionType
     * @pdOid 8612d1fe-6b34-4f88-9e2c-08b5c8ecfefc
     * @date 2016年11月14日
     */
    ActionType getAction();

    /**
     * 注入要操作的行为
     *
     * @param actionType 操作类型
     * @pdOid f23044a7-9afa-4f55-85a1-3b241bcfe576
     * @date 2016年11月15日
     */
    Action setAction(ActionType actionType);

    public static final class selectAction implements Action {
        public ActionType actionType;

        public selectAction(ActionType actionType) {
            this.actionType = actionType;
        }

        @Override
        public ActionType getAction() {
            return actionType;
        }

        @Override
        public Action setAction(ActionType actionType) {
            this.actionType = actionType;
            return this;
        }
    }

    enum ActionType {
        /**
         * 表格创建
         */
        CREATE_TABLE,
        /**
         * 表格删除
         */
        DELECT_TABLE,
        /**
         * 表格更新
         */
        UPDATE_TABLE,
        /**
         * 表格搜索
         */
        SELECT_TABLE,
        /**
         * 插入数据
         */
        INSERT_DATA,
        /**
         * 删除数据
         */
        DELECT_DATA,
        /**
         * 更新数据
         */
        UPDATE_DATA,
        /**
         * 搜索数据
         */
        SELECT_DATA

    }

}