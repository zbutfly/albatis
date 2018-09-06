package intercepet;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.hzcominfo.albatis.search.auth.AuthHandler;
import com.hzcominfo.albatis.search.result.ResultSetBase;
import net.butfly.albacore.utils.logger.Loggable;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class AuthorizationIntercepet implements AuthHandler, Loggable {

    @Override
    public ResultSetBase auth(ResultSetBase rs, String uri, String[] tables, String username) throws IllegalArgumentException, SQLException {
        return (ResultSetBase) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[]{ResultSetBase.class},
                new ResultSetHandler(rs, uri, tables, username));
    }

    private JSONArray authFields(String uri, String[] tables, String username) {
        JSONObject jo = new JSONObject();
        jo.put("uri", uri);
        jo.put("table", tables);
        jo.put("username", username);
        return DAConfig.filter(jo.toJSONString());
    }

    public class ResultSetHandler implements InvocationHandler {
        private ResultSetBase rs;
        private String uri;
        private String[] tables;
        private String username;

        public ResultSetHandler(ResultSetBase rs, String uri, String[] tables, String username) throws SQLException {
            super();
            this.rs = rs;
            this.uri = uri;
            this.tables = tables;
            this.username = username;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            Object invoke = method.invoke(rs, args);
            List<Map<String, Object>> resultMapList = rs.getResultMapList();
            if (!"getResultMapList".equals(method.getName()) || null == resultMapList || resultMapList.size() < 1)
                return invoke;
            JSONArray fields = authFields(uri, tables, username);
            for (Map<String, Object> resultMap : resultMapList) {
                for (Entry<String, Object> entry : resultMap.entrySet()) {
                    resultMap.put(entry.getKey(), fields == null || fields.contains(entry.getKey()) ? entry.getValue() : "***");
                }
            }
            return resultMapList;
        }
    }
}
