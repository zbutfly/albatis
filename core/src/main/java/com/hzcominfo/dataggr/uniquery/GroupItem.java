package com.hzcominfo.dataggr.uniquery;

public class GroupItem {

	private String groupField;

	public GroupItem(String groupField) {
		this.groupField = groupField;
	}

	public static GroupItem of(String groupField) {
		return new GroupItem(groupField);
	}

	public String name() {
		return groupField;
	}
}
