package net.butfly.albatis.hmpp;

import com.hmpp.hmppanalyticsdb.load.CDCAdapterData;
import com.hmpp.hmppanalyticsdb.load.ConnInfo;
import com.hmpp.hmppanalyticsdb.load.CopyLoad;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.io.OutputBase;
import net.butfly.albatis.io.Rmap;

import java.util.ArrayList;
import java.util.Map;

public class HmppOutput extends OutputBase<Rmap> {
    static final Logger logger = Logger.getLogger(HmppOutput.class);
    HmppConnection conn;


    public HmppOutput(HmppConnection hmppConnection){
        super("HmppOutput");
        this.conn = hmppConnection;
    }

    private CDCAdapterData init(int op){
        CDCAdapterData cdcAdapterData = new CDCAdapterData();
        cdcAdapterData.operType = op;
        cdcAdapterData.operTypeList = new ArrayList<Integer>();
        cdcAdapterData.valueList = new ArrayList<Map<String, Object>>();
        return cdcAdapterData;
    }


    @Override
    protected void enqsafe(Sdream<Rmap> sdream) {
        Map<String, Hmpp> poolMap = Maps.of();
        sdream.eachs(m -> {
            if(!poolMap.containsKey(m.table().name)){
                ConnInfo connInfo = conn.connInfo;
                connInfo.tableName = m.table().name;
                connInfo.matchColumn = new String[] { m.keyField() };
                int op = getOp(m.op());
                CDCAdapterData cdcAdapterData = init(op);
                cdcAdapterData.operTypeList.add(op);
                Hmpp hmpp = new Hmpp(connInfo,cdcAdapterData);
                poolMap.put(m.table().name,hmpp);
            }
            poolMap.get(m.table().name).cdcAdapterData.operTypeList.add(getOp(m.op()));
            poolMap.get(m.table().name).cdcAdapterData.valueList.add(m);
        });
        try {
            for(Map.Entry<String, Hmpp> entry : poolMap.entrySet()){
                CopyLoad.LoadAdapterData(entry.getValue().cdcAdapterData, entry.getValue().connInfo);
            }
        } catch (Exception e) {
            logger.error("upsert hmpp error: "+ e);
        }
    }

    private int getOp(int op){
        switch (op) {
            case Rmap.Op.INSERT:
                return 1;
            case Rmap.Op.UPDATE:
                return 2;
            case Rmap.Op.DELETE:
                return 3;
            case Rmap.Op.UPSERT:
                return 4;
                default:
                    return 4;
        }
    }

    private class Hmpp {
        CDCAdapterData cdcAdapterData;
        ConnInfo connInfo;

        private Hmpp(ConnInfo connInfo, CDCAdapterData cdcAdapterData){
            this.cdcAdapterData = cdcAdapterData;
            this.connInfo = connInfo;
        }
    }
}
