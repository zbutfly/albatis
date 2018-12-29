package net.butfly.albatis.ddl;

import java.util.List;
import java.util.Map;

public class TableCustomSet {
    //其他设置-与不同数据源有关的设置
    private Map<String, Object> options;
    //键的设置
    private List<List<String>> keys;
    //索引的设置
    private List<List<String>> indexes;

    public void setOptions(Map<String, Object> options) {
        this.options = options;
    }

    public void setKeys(List<List<String>> keys) {
        this.keys = keys;
    }

    public void setIndexes( List<List<String>> indexes) {
        this.indexes = indexes;
    }

    public Map<String, Object> getOptions() {
        return options;
    }

    public List<List<String>> getKeys() {
        return keys;
    }

    public  List<List<String>> getIndexes() {
        return indexes;
    }
}
