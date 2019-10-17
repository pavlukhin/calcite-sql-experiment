package org.ivan.experiments.sql;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class SqlTuple {
    private final Map<String, Object> cols;

    public SqlTuple(Map<String, Object> cols) {
        this.cols = cols;
    }

    public Object column(String name) {
        return cols.get(name);
    }
    public Object column(int i) {
        Iterator<Object> it = cols.values().iterator();
        Object ret = it.next();
        for (int j = 0; j < i; j++) {
            ret = it.next();
        }
        return ret;
    }

    public Set<String> columnNames() {
        return cols.keySet();
    }
}
