package net.yck.wrkdb.server.store;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;

import net.yck.wkrdb.common.ByteArrayWrapper;
import net.yck.wkrdb.common.DBException;
import net.yck.wrkdb.server.meta.Table;

class CacheStore extends Store {

  private Store                                            backend;
  private Cache<ByteArrayWrapper, Map<String, ByteBuffer>> incoming;
  private Cache<ByteArrayWrapper, Map<String, ByteBuffer>> outgoing;

  protected CacheStore(Table table) {
    super(table);
  }

  CacheStore setBackend(Store backend) {
    this.backend = backend;
    return this;
  }

  @Override
  protected Store doOpen() throws DBException {

    if (backend != null)
      backend.doOpen();
    
    incoming = Caffeine.newBuilder().maximumSize(10000).expireAfterWrite(5, TimeUnit.MINUTES)
        .removalListener(new RemovalListener<ByteArrayWrapper, Map<String, ByteBuffer>>() {
          @Override
          public void onRemoval(ByteArrayWrapper key, Map<String, ByteBuffer> value, RemovalCause cause) {
            backend.put(PutOptions.c_default, key.array(), value);
          }
        }).build();

    outgoing = Caffeine.newBuilder().maximumSize(10000).expireAfterWrite(5, TimeUnit.MINUTES).build();

    return this;
  }

  @Override
  protected Store doClose() throws DBException {
    if (backend != null)
      backend.doClose();
    return this;
  }

  @Override
  public List<ByteBuffer> get(GetOptions options, byte[] rowKey, List<String> groups) throws DBException {
    if (options.getUseCache()) {
      final ByteArrayWrapper baw = new ByteArrayWrapper(rowKey);
      List<ByteBuffer> ret = new ArrayList<>();
      groups.forEach(grp -> {
        ByteBuffer buffer = null;

        Map<String, ByteBuffer> incomingMappings = incoming.getIfPresent(baw);
        if (incomingMappings != null) {
          synchronized (incomingMappings) {
            buffer = incomingMappings.get(grp);
          }
        }

        if (buffer == null) {
          Map<String, ByteBuffer> outgoingMappings = outgoing.get(baw, k -> new HashMap<String, ByteBuffer>());
          synchronized (outgoingMappings) {
            buffer = outgoingMappings.computeIfAbsent(grp,
                x -> backend.get(GetOptions.c_default, rowKey, Arrays.asList(x)).get(0));
          }
        }

        ret.add(buffer == Store.c_RemovalIndicator ? null : buffer);
      });

      return ret;
    } else {
      return backend.get(options, rowKey, groups);
    }
  }

  @Override
  public void put(PutOptions options, byte[] rowKey, Map<String, ByteBuffer> row) throws DBException {
    if (options.getUseCache()) {
      final ByteArrayWrapper baw = new ByteArrayWrapper(rowKey);
      Map<String, ByteBuffer> incomingMappings = incoming.get(baw, k -> row);
      if (incomingMappings != row) {
        synchronized (incomingMappings) {
          row.forEach((col, val) -> incomingMappings.put(col, val));
        }
      }

      Map<String, ByteBuffer> outgoingMappings = outgoing.getIfPresent(baw);
      if (outgoingMappings != null) {
        synchronized (outgoingMappings) {
          row.keySet().forEach((col) -> outgoingMappings.remove(col));
        }
      }
    } else {
      backend.put(options, rowKey, row);
    }
  }

  @Override
  public void remove(PutOptions options, byte[] rowKey, List<String> groups) throws DBException {
    if (options.getUseCache()) {
      final ByteArrayWrapper baw = new ByteArrayWrapper(rowKey);
      Map<String, ByteBuffer> mappings = incoming.get(baw, k -> new HashMap<String, ByteBuffer>());
      synchronized (mappings) {
        groups.forEach(x -> mappings.put(x, Store.c_RemovalIndicator));
      }
    } else {
      backend.remove(options, rowKey, groups);
    }
  }
}
