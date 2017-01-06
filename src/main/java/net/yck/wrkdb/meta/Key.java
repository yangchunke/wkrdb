package net.yck.wrkdb.meta;

import org.mapdb.DBException;

abstract class Key extends Group {

    public final static class Partition extends Key {

        public Partition() {
            super.setName(kw_PartitionKey);
        }

        @Override
        public NamedElement setName(String name) {
            throw new DBException("The name of partition key cannot be changed.");
        }
    }

    public final static class Row extends Key {
        public Row() {
            super.setName(kw_RowKey);
        }

        @Override
        public NamedElement setName(String name) {
            throw new DBException("The name of row key cannot be changed.");
        }
    }

    public final static class Clustering extends Key {
    }
}
