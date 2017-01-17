package net.yck.wrkdb.server.store;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import net.yck.wrkdb.common.DBException;
import net.yck.wrkdb.server.meta.Table;

class CacheStore extends Store {
  protected final Store      backend;

  Cache<byte[], byte[]> graphs = Caffeine.newBuilder().maximumSize(10000).expireAfterWrite(5, TimeUnit.MINUTES)
      .build();

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
  public List<ByteBuffer> get(GetOptions options, byte[] rowKey, List<String> groups) throws DBException {
    return backend.get(options, rowKey, groups);
  }

  @Override
  public void put(PutOptions options, byte[] rowKey, Map<String, ByteBuffer> row) throws DBException {
    backend.put(options, rowKey, row);
  }

  @Override
  public void remove(PutOptions options, byte[] rowKey, List<String> groups) throws DBException {
    backend.remove(options, rowKey, groups);
  }
}
