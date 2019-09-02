package net.butfly.albatis.io.format;

import net.butfly.albatis.io.Rmap;
import net.butfly.alserdes.SerDes;

import java.util.UUID;

@SerDes.As("desers")
public class RmapDesersFormat extends RmapFormat {


    @Override
    public Rmap ser(Rmap rmap) {
        return rmap;
    }

    @Override
    public Rmap deser(Rmap rmap) {
        if(null == rmap)return null;
        Rmap rmap1 = new Rmap();
        rmap1.put(UUID.randomUUID().toString(),rmap);
        rmap1.table(rmap.table()).key(rmap.key());
        return rmap1;

    }
}
