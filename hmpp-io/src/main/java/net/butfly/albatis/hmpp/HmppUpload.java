package net.butfly.albatis.hmpp;

import com.hmpp.hmppanalyticsdb.load.CDCAdapterData;
import com.hmpp.hmppanalyticsdb.load.ConnInfo;
import com.hmpp.hmppanalyticsdb.load.CopyLoad;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.Rmap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static net.butfly.albacore.utils.logger.StatsUtils.formatMillis;
import static net.butfly.albatis.hmpp.HmppOutput.logger;

public class HmppUpload {
    private static final AtomicLong FIELD_SPENT = new AtomicLong(), REC_COUNT = new AtomicLong();
    private static final long START = System.currentTimeMillis();

    HmppUpload(){
        super();
    }
    private CDCAdapterData init(int op){
        CDCAdapterData cdcAdapterData = new CDCAdapterData();
        cdcAdapterData.operType = op;
        cdcAdapterData.operTypeList = new ArrayList<Integer>();
        cdcAdapterData.valueList = new ArrayList<Map<String, Object>>();
        return cdcAdapterData;
    }

    void upload(List<Rmap> rmaps,String tableName,ConnInfo info){
        Map<String, Hmpp> poolMap = Maps.of();
        long spent = System.currentTimeMillis();
        rmaps.forEach(m -> {
            if(!poolMap.containsKey(m.table().name)){
                ConnInfo connInfo = info;
                connInfo.tableName = m.table().name;
                connInfo.matchColumn = new String[] { m.keyField() };
                int op = getOp(m.op());
                CDCAdapterData cdcAdapterData = init(op);
                cdcAdapterData.operTypeList.add(op);
                Hmpp hmpp = new Hmpp(connInfo,cdcAdapterData);
                poolMap.put(m.table().name,hmpp);
            }
            REC_COUNT.incrementAndGet();
            poolMap.get(m.table().name).cdcAdapterData.operTypeList.add(getOp(m.op()));
            poolMap.get(m.table().name).cdcAdapterData.valueList.add(m);
        });
        try {
            for(Map.Entry<String, Hmpp> entry : poolMap.entrySet()){
                CopyLoad.LoadAdapterData(entry.getValue().cdcAdapterData, entry.getValue().connInfo);
            }
        } catch (Exception e) {
            logger.error("upsert hmpp error: "+ e);
        }finally {
            long spent2 = FIELD_SPENT.addAndGet(System.currentTimeMillis() - spent);
            long spentAll = System.currentTimeMillis() - START;
            long c = REC_COUNT.intValue();
            if (c % 10000 == 0) logger.trace("avg field proc time of [" + c + "] insert avg: " + (c * 1000 / spent2)
                    + " recs/s, output avg: "+ (c * 1000 / spentAll) + " recs/s,spend:"+formatMillis(spentAll));
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
