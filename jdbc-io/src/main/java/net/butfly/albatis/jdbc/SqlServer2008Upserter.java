package net.butfly.albatis.jdbc;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.io.Message;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

public class SqlServer2008Upserter extends Upserter {
    public SqlServer2008Upserter(Type type) {
        super(type);
    }

    @Override
    String urlAssemble(URISpec uriSpec) {
        return null;
    }

    @Override
    String urlAssemble(String schema, String host, String database) {
        return null;
    }

    @Override
    long upsert(Map<String, List<Message>> mml, Connection conn) {
        return 0;
    }

}
