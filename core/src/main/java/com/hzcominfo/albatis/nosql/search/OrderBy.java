package com.hzcominfo.albatis.nosql.search;

/**
 * Created by ljx on 2016/11/23.
 *
 * @author ljx
 * @version 0.0.1
 */
public interface OrderBy {
    public static final class Sort implements OrderBy {
        public String field;
        public Order order;

        public Sort(String field, Order order) {
            this.field = field;
            this.order = order;

        }
    }

    public static enum Order {
        ASC, DESC
    }
}
