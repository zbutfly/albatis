package net.butfly.albatis.jdbc;

import net.butfly.albacore.base.Namedly;
import net.butfly.albatis.io.Message;
import net.butfly.albatis.io.OddInput;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JdbcTestInput extends Namedly implements OddInput<Message> {
    private List<Message> list;

    public JdbcTestInput(String name) {
        super(name);
        list = Stream.iterate(1, i -> i + 1).limit(20).map(i -> {
            Message m = new Message("test");
            m.put("id", i);
            m.key(i);
//            m.put("name", UUID.randomUUID().toString());
            if (i % 2 == 0) m.put("address", UUID.randomUUID().toString());
            if (i % 5 == 0) m.put("id", "" + i);
            return m;
        }).collect(Collectors.toList());
        open();
    }

    @Override
    public Message dequeue() {
        return list.size() > 0 ? list.remove(0) : null;
    }

    @Override
    public boolean empty() {
        return list.size() <= 0;
    }
}
