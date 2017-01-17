package net.yck.wrkdb.server.store;

import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.mapdb.serializer.SerializerCompressionWrapper;

import net.yck.wkrdb.server.db.DBOptions;
import net.yck.wrkdb.common.DBException;
import net.yck.wrkdb.common.util.ByteBufferUtil;
import net.yck.wrkdb.server.meta.Table;
import net.yck.wrkdb.server.util.Sharder;

public class MapDBStore extends FileStore {

  private static String c_Suffix = ".mdb";
  private static String c_Meta   = "meta";
  private static String c_Data   = "data";

  private DB            meta;
  private MapProvider   mapProvider;

  protected MapDBStore(Table table) {
    super(table);
  }

  @Override
  public Store doOpen() throws DBException {
    meta = DBMaker.fileDB(Paths.get(folder(), c_Meta + c_Suffix).toString())//
        .make();
    mapProvider = new MapProvider(getOptions());
    return this;
  }

  @Override
  public Store doClose() throws DBException {
    if (meta != null) {
      meta.close();
      meta = null;
    }
    mapProvider.close();
    return this;
  }

  @Override
  public List<ByteBuffer> get(GetOptions options, byte[] rowKey, List<String> groups) throws DBException {
    List<ByteBuffer> ret = new ArrayList<>();
    for (String group : groups) {
      ret.add(ByteBufferUtil.fromByteArray(map(rowKey, group).get(rowKey)));
    }
    return ret;
  }

  @Override
  public void put(PutOptions options, byte[] rowKey, Map<String, ByteBuffer> row) throws DBException {
    for (Entry<String, ByteBuffer> entry : row.entrySet()) {
      if (entry.getValue() != null) {
        map(rowKey, entry.getKey()).put(rowKey, entry.getValue().array());
      } else {
        map(rowKey, entry.getKey()).remove(rowKey);
      }
    }
  }

  @Override
  public void remove(PutOptions options, byte[] rowKey, List<String> groups) throws DBException {
    for (String group : groups) {
      map(rowKey, group).remove(rowKey);
    }
  }

  private Map<byte[], byte[]> map(byte[] key, String group) throws DBException {
    return mapProvider.map(key, group);
  }

  // ---------------------------------------------------------------------------

  private class MapProvider {

    final Sharder sharder;
    final DB      dbs[];

    MapProvider(DBOptions options) {
      sharder = Sharder.getSharder(options);
      int numOfShards = options.getNumOfShards();
      dbs = new DB[numOfShards];
    }

    Map<byte[], byte[]> map(byte[] key, String group) throws DBException {
      int shard = sharder.shard(key, dbs.length);
      return db(shard).treeMap(group)//
          .valuesOutsideNodesEnable()//
          .keySerializer(Serializer.BYTE_ARRAY)//
          .valueSerializer(new SerializerCompressionWrapper<>(Serializer.BYTE_ARRAY))//
          .createOrOpen();
    }

    private void close() throws DBException {
      for (int shard = 0; shard < dbs.length; shard++) {
        DB db = dbs[shard];
        if (db != null) {
          db.close();
          LOG.info(file(shard) + " closed.");
        }
      }
    }

    private DB db(int shard) throws DBException {
      if (dbs[shard] == null) {
        synchronized (dbs) {
          if (dbs[shard] == null) {
            dbs[shard] = DBMaker.fileDB(file(shard))//
                .fileMmapEnable()//
                .make();
            LOG.info(file(shard) + " opened");
          }
        }
      }
      return dbs[shard];
    }

    private String file(int shard) throws DBException {
      return Paths.get(folder(), name(shard)).toString();
    }

    private String name(int shard) {
      return String.format("%s_%02d%s", c_Data, shard, c_Suffix);
    }
  }

}
