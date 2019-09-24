package net.butfly.albatis.elastic;

import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.serder.BsonSerder;
import net.butfly.albacore.utils.IOs;
import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.io.Rmap;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

public class ElasticMessage extends Rmap {
    private static final long serialVersionUID = -125189207796104302L;
    transient Script script;

    public ElasticMessage(String index, String id, Script script, Map<String, Object> upsertDoc) {
        super((Qualifier) null, id, upsertDoc);
        table(new Qualifier(index));
        op(null == upsertDoc ? Op.UPDATE : Op.UPSERT);
        this.script = script;
    }

    public ElasticMessage(String index, String id, Script script) {
        this(index, id, script, null);
    }

    public String index() {
        return table.name;
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(Rmap.opname(op)).append("@").append(table).append("/").append(key);
        if (script == null) sb.append("(upsert:").append("):\n\tdocument:").append(this);
        else sb.append(":\n\tscript: ").append(script);
        return sb.toString();
    }

    @Override
    protected void write(OutputStream os, Function<Map<String, Object>, byte[]> conv) throws IOException {
        super.write(os, conv);
        boolean s = null == script;
        IOs.writeBytes(os, new byte[]{(byte) (s ? 0 : 1)});
        if (s)
            IOs.writeBytes(os, script.getType().name().getBytes(), script.getLang().getBytes(), script.getIdOrCode().getBytes(), BsonSerder
                    .map(script.getParams()));
    }

    public ElasticMessage(byte[] b) throws IOException {
        this(new ByteArrayInputStream(b));
    }

    public ElasticMessage(InputStream is) throws IOException {
        super(is, BsonSerder::map);
        if (0 != IOs.readBytes(is)[0]) {
            byte[][] attrs = IOs.readBytesList(is);
            this.script = new Script(ScriptType.valueOf(new String(attrs[0])), new String(attrs[1]), new String(attrs[2]), BsonSerder.map(
                    attrs[3]));
        }
    }
}
