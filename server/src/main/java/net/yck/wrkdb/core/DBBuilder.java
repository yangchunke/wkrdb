package net.yck.wrkdb.core;

import net.yck.wrkdb.common.DBException;
import net.yck.wrkdb.meta.Catalog;

public abstract class DBBuilder<T> {
    protected Catalog catalog;
    protected DBOptions options;

    public abstract T build() throws DBException;

    public DBBuilder<T> setCatalog(Catalog catalog) {
        this.catalog = catalog;
        return this;
    }

    public DBBuilder<T> setOptions(DBOptions options) {
        this.options = options;
        return this;
    }
}
