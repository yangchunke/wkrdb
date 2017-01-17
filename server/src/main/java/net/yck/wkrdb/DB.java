package net.yck.wkrdb;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import net.yck.wrkdb.core.DBBuilder;
import net.yck.wrkdb.core.DBException;
import net.yck.wrkdb.core.DBOptions;
import net.yck.wrkdb.meta.Catalog;
import net.yck.wrkdb.meta.Schema;
import net.yck.wrkdb.meta.Table;
import net.yck.wrkdb.store.Store;

public class DB implements AutoCloseable {

  final static Logger                           LOG    = LoggerFactory.getLogger(DB.class);

  private Catalog                               catalog;
  private DBOptions                             options;
  private final Map<String, Map<String, Store>> stores = new ConcurrentHashMap<>();

  @Override
  public void close() throws Exception {
    for (Map<String, Store> sub : stores.values()) {
      for (Store store : sub.values()) {
        try {
          store.close();
        } catch (DBException e) {
          LOG.error("failed to close " + store.identifier(), e);
        }
      }
    }
  }

  Catalog getCatalog() {
    return catalog;
  }

  DB setCatalog(Catalog catalog) {
    this.catalog = catalog;
    return this;
  }

  DBOptions getOptions() {
    return options;
  }

  DB setOptions(DBOptions options) {
    this.options = options;
    return this;
  }

  public Store getStore(String schema, String table) throws DBException {
    Schema schema_ = catalog.getSchema(schema);
    if (schema_ == null)
      throw new DBException("Schema [" + schema + "] does not exist.");

    Table table_ = schema_.getTable(table);
    if (table_ == null)
      throw new DBException("Table [" + table + "] does not exist.");
    if (schema_ != table_.getSchema())
      throw new DBException("Table [" + table + "] does not belong to schema [ " + schema + "].");

    Map<String, Store> sub = stores.computeIfAbsent(schema, s -> {
      return new ConcurrentHashMap<>();
    });

    Store store = sub.computeIfAbsent(table, t -> {
      try {
        return Store.builder()//
            .setTable(table_)//
            .setOptions(options)//
            .build();
      } catch (DBException e) {
        LOG.error("getStore", e);
      }
      return null;
    });

    if (store == null) {
      throw new DBException("Failed to obtain the store for Table [" + table + "]");
    }

    return store.open();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder extends DBBuilder<DB> {

    @SuppressWarnings("resource")
    @Override
    public DB build() {
      Preconditions.checkNotNull(catalog);
      return new DB().setOptions(options).setCatalog(catalog);
    }
  }

}
