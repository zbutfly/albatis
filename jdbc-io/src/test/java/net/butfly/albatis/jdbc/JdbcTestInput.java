package net.butfly.albatis.jdbc;

import net.butfly.albacore.base.Namedly;
import net.butfly.albatis.io.R;
import net.butfly.albatis.io.OddInput;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JdbcTestInput extends Namedly implements OddInput<R> {
    private List<R> list;

    /**
     * for mysql, all field should be upper case
     * @param name
     */
    public JdbcTestInput(String name) {
        super(name);
        list = Stream.iterate(1, i -> i + 1).limit(20).map(i -> {
            R m = new R("ATEST");
            m.put("ID", i);
            m.key(i);
            m.put("NAME", UUID.randomUUID().toString());
            if (i % 2 == 0) m.put("ADDRESS", UUID.randomUUID().toString());
            return m;
        }).collect(Collectors.toList());
        open();
    }

    @Override
    public R dequeue() {
        return list.size() > 0 ? list.remove(0) : null;
    }

    @Override
    public boolean empty() {
        return list.size() <= 0;
    }
}
