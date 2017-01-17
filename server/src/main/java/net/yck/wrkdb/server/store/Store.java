package net.yck.wrkdb.server.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

import net.yck.wkrdb.server.db.DBBuilder;
import net.yck.wkrdb.server.db.DBOptions;
import net.yck.wkrdb.server.db.DBState;
import net.yck.wrkdb.common.DBException;
import net.yck.wrkdb.common.util.AvroUtil;
import net.yck.wrkdb.common.util.ByteBufferUtil;
import net.yck.wrkdb.server.meta.Group;
import net.yck.wrkdb.server.meta.Meta;
import net.yck.wrkdb.server.meta.Table;

public abstract class Store {

  final protected static Logger LOG = LoggerFactory.getLogger(Store.class);

  public static enum Type {
    MapDB
  }

  protected final Table                       table;
  private DBOptions                           options;
  private DBState                             state;

  private org.apache.avro.Schema              schemaAvroSchema;
  private org.apache.avro.Schema              partitionKeyAvroSchema;
  private org.apache.avro.Schema              rowkeyAvroSchema;
  private Map<String, org.apache.avro.Schema> groupAvroSchemaMap;

  protected Store(Table table) {
    this.table = table;
  }

  private Store initialize() {
    schemaAvroSchema = new org.apache.avro.Schema.Parser().parse(table.getSchema().toAvro());

    partitionKeyAvroSchema = schemaAvroSchema.getTypes()
        .get(schemaAvroSchema.getIndexNamed(table.getPartitionKey().getNamespace() + "." + Meta.kw_PartitionKey));
    if (table.getRowKey() != null) {
      rowkeyAvroSchema = schemaAvroSchema.getTypes()
          .get(schemaAvroSchema.getIndexNamed(table.getRowKey().getNamespace() + "." + Meta.kw_RowKey));
    } else {
      rowkeyAvroSchema = partitionKeyAvroSchema;
    }

    groupAvroSchemaMap = new HashMap<>();
    for (Group group : table.getGroups()) {
      Integer groupIdx = schemaAvroSchema.getIndexNamed(group.getNamespace() + "." + group.getName());
      groupAvroSchemaMap.put(group.getName(), schemaAvroSchema.getTypes().get(groupIdx));
    }

    return this;
  }

  public org.apache.avro.Schema getSchemaAvroSchema() {
    return schemaAvroSchema;
  }

  public org.apache.avro.Schema getPartitionKeyAvroSchema() {
    return partitionKeyAvroSchema;
  }

  public org.apache.avro.Schema getRowKeyAvroSchema() {
    return rowkeyAvroSchema;
  }

  public Map<String, org.apache.avro.Schema> getGroupAvroSchemaMap() {
    return groupAvroSchemaMap;
  }

  protected abstract Store doOpen() throws DBException;

  public Store open() throws DBException {
    if (DBState.Closed == getState()) {
      synchronized (this) {
        if (DBState.Closed == getState()) {
          initialize().doOpen().setState(DBState.Open);
          LOG.info(identifier() + " opened.");
        }
      }
    }
    if (DBState.Open == getState()) {
      return this;
    }
    throw new DBException(identifier() + " is in bad state " + state);
  }

  protected abstract Store doClose() throws DBException;

  public void close() throws DBException {
    if (DBState.Open == getState()) {
      synchronized (this) {
        if (DBState.Open == getState()) {
          doClose().setState(DBState.Closed);
          LOG.info(identifier() + " closed.");
        }
      }
    }
    if (DBState.Closed != getState()) {
      throw new DBException(identifier() + " is in bad state " + state);
    }
  }

  public abstract List<ByteBuffer> get(GetOptions options, byte[] rowKey, List<String> groups) throws DBException;

  public List<GenericRecord> get(GetOptions options, GenericRecord rowKey, List<String> groups) throws DBException {
    List<GenericRecord> ret = new ArrayList<>();
    try {
      byte[] rowKeyBytes = AvroUtil.toBytes(rowKey, rowkeyAvroSchema);
      List<ByteBuffer> raw = get(options, rowKeyBytes, groups);
      Preconditions.checkState(raw.size() == groups.size());
      for (int i = 0; i < raw.size(); i++) {
        GenericRecord rec = null;
        byte[] bytes = ByteBufferUtil.toByteArray(raw.get(i));
        if (bytes != null) {
          org.apache.avro.Schema groupAvroSchema = groupAvroSchemaMap.get(groups.get(i));
          if (groupAvroSchema != null) {
            try {
              rec = AvroUtil.fromBytes(bytes, groupAvroSchema);
            } catch (IOException e) {
              LOG.warn(e.getMessage());
            }
          }
        }
        ret.add(rec);
      }
    } catch (IOException e) {
      throw new DBException(e);
    }

    Preconditions.checkState(ret.size() == groups.size());
    return ret;
  }

  public abstract void put(PutOptions options, byte[] rowKey, Map<String, ByteBuffer> row) throws DBException;

  public void put(PutOptions options, GenericRecord rowKey, Map<String, GenericRecord> record) throws DBException {
    try {
      byte[] rowKeyBytes = AvroUtil.toBytes(rowKey, rowkeyAvroSchema);
      Map<String, ByteBuffer> raw = new HashMap<>();
      for (Map.Entry<String, GenericRecord> entry : record.entrySet()) {
        if (entry.getValue() != null) {
          org.apache.avro.Schema groupAvroSchema = groupAvroSchemaMap.get(entry.getKey());
          if (groupAvroSchema != null) {
            raw.put(entry.getKey(), ByteBufferUtil.fromByteArray(AvroUtil.toBytes(entry.getValue(), groupAvroSchema)));
          }
        }
      }
      put(options, rowKeyBytes, raw);
    } catch (IOException e) {
      throw new DBException(e);
    }
  }

  public abstract void remove(PutOptions options, byte[] rowKey, List<String> groups) throws DBException;

  public void remove(PutOptions options, GenericRecord rowKey, List<String> groups) throws DBException {
    try {
      byte[] rowKeyBytes = AvroUtil.toBytes(rowKey, rowkeyAvroSchema);
      remove(options, rowKeyBytes, groups);
    } catch (IOException e) {
      throw new DBException(e);
    }
  }

  public final String identifier() {
    return identifier(':');
  }

  public final String identifier(char separator) {
    return Joiner.on(separator).join(table.getSchema().getCatalog().getName(), table.getSchema().getName(),
        table.getName());
  }

  DBOptions getOptions() {
    return this.options;
  }

  Store setOptions(DBOptions options) {
    this.options = options;
    return this;
  }

  public DBState getState() {
    return this.state;

  }

  Store setState(DBState state) {
    this.state = state;
    return this;
  }

  // ------------------------------------------------------------------------------------------

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder extends DBBuilder<Store> {

    private Table table;

    @Override
    public Store build() throws DBException {
      Store ret = null;

      switch (options.getStoreType()) {
        case MapDB:
          ret = new MapDBStore(table).setOptions(options).setState(DBState.Closed);
          break;
        default:
          throw new DBException("unsupported store type - " + options.getStoreType());
      }

      if (ret != null && options.getUseCache()) {
        ret = new CacheStore(table, ret).setOptions(options).setState(DBState.Closed);
      }

      return ret;
    }

    public Builder setTable(Table table) {
      this.table = table;
      return this;
    }
  }
}
