package com.hzcominfo.dataggr.uniquery.field_maskBean;

import java.util.List;

public class FieldBean {
	private int field_id;
	private List<FieldBeanX> field;
	
	public int getField_id() {
		return field_id;
	}
	public void setField_id(int field_id) {
		this.field_id = field_id;
	}
	public List<FieldBeanX> getField() {
		return field;
	}
	public void setField(List<FieldBeanX> field) {
		this.field = field;
	}

	
}
