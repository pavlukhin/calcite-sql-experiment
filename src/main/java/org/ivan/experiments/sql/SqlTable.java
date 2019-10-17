package org.ivan.experiments.sql;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public interface SqlTable extends Iterable<SqlTuple> {
    default Stream<SqlTuple> stream() {
        return StreamSupport.stream(spliterator(), false);
    }
}
