package net.yck.wrkdb.meta;

import java.util.Set;
import java.util.TreeSet;

public interface Meta {

    public final static String kw_PartitionKey = "PartitionKey";
    public final static String kw_RowKey = "RowKey";

    static Set<String> Reserved_Keywords = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER) {
        private static final long serialVersionUID = 9117825108116596290L;
        {
            add(kw_PartitionKey);
            add(kw_RowKey);
        }
    };
}
