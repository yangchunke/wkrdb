package net.yck.wrkdb.store;

import java.util.List;
import java.util.Map;

import net.yck.wrkdb.core.DBException;
import net.yck.wrkdb.meta.Table;

class CacheStore extends Store {
  protected final Store backend;

  protected CacheStore(Table table, Store backend) {
    super(table);
    this.backend = backend;
  }

  @Override
  protected Store doOpen() throws DBException {
    backend.doOpen();
    return this;
  }

  @Override
  protected Store doClose() throws DBException {
    backend.doClose();
    return this;
  }

  @Override
  public List<byte[]> get(GetOptions options, byte[] rowKey, List<String> groups) throws DBException {
    return backend.get(options, rowKey, groups);
  }

  @Override
  public void put(PutOptions options, byte[] rowKey, Map<String, byte[]> row) throws DBException {
    backend.put(options, rowKey, row);    
  }

  @Override
  public void remove(PutOptions options, byte[] rowKey, List<String> groups) throws DBException {
    backend.remove(options, rowKey, groups);
  }
}
