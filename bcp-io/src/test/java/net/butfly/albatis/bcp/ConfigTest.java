package net.butfly.albatis.bcp;

import net.butfly.albacore.utils.Configs;

public class ConfigTest {
    public static void main(String[] args) {
        String taskSql = getTaskSql();
        System.out.println(taskSql);
    }

    private static String getTaskSql(){
        return Configs.of("bcpsql.properties","dataggr.migrate").get("bcp.sql.task");
//        String fieldSql = Configs.of("bcpsql.properties", "dataggr.migrate").get("bcp.sql.fields");
    }
}
