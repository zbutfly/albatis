package net.butfly.albatis.ddl;

import net.butfly.albatis.ddl.vals.ValType;

public class Field {
    //属性名 如id,name
    private String fieldName;
    //数据类型+字段长度  如VARCHAR(20),INT(16)
    private ValType type;

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public void setType(ValType type) {
        this.type = type;
    }

    public ValType getType() {
        return type;
    }

    public String getFieldName() {
        return fieldName;
    }



}
