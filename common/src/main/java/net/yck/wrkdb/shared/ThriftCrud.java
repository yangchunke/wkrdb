package net.yck.wrkdb.shared;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TException;

import com.google.common.base.Preconditions;

import net.yck.wrkdb.core.DBException;
import net.yck.wrkdb.service.thrift.DBContext;
import net.yck.wrkdb.service.thrift.DBSchema;
import net.yck.wrkdb.service.thrift.DbService;
import net.yck.wrkdb.service.thrift.Key;
import net.yck.wrkdb.util.AvroUtil;
import net.yck.wrkdb.util.ByteBufferUtil;

public class ThriftCrud {

  private final DbService.Iface                            iface;

  private final static Map<String, DBSchema>               schemas            = new ConcurrentHashMap<>();
  private final static Map<String, org.apache.avro.Schema> groupAvroSchemaMap = new ConcurrentHashMap<>();

  public ThriftCrud(DbService.Iface client) {
    this.iface = client;
  }

  private org.apache.avro.Schema getGroupAvroSchema(final DBContext context, final String group) throws DBException {
    final String key = context.identifier + "." + group;
    return groupAvroSchemaMap.computeIfAbsent(key, x -> {
      DBSchema schema = getSchema(context);
      return new org.apache.avro.Schema.Parser().parse(schema.columnGroupSchemaMap.get(group));
    });
  }

  private DBSchema getSchema(final DBContext context) throws DBException {

    final DBException exp = new DBException(StringUtils.EMPTY);

    DBSchema ret = schemas.computeIfAbsent(context.identifier, key -> {
      try {
        return iface.getDBSchema(context);
      } catch (TException e) {
        exp.initCause(e);
      }
      return null;
    });

    if (ret == null) {
      throw exp;
    }

    return ret;
  }

  public void put(DBContext context, Key key, Map<String, GenericRecord> mappings) throws DBException {
    try {
      Map<String, ByteBuffer> raw = new HashMap<>();
      for (Map.Entry<String, GenericRecord> entry : mappings.entrySet()) {
        if (entry.getValue() != null) {
          org.apache.avro.Schema groupAvroSchema = getGroupAvroSchema(context, entry.getKey());
          if (groupAvroSchema != null) {
            raw.put(entry.getKey(), ByteBufferUtil.fromByteArray(AvroUtil.toBytes(entry.getValue(), groupAvroSchema)));
          }
        }
      }
      iface.put(context, key, raw);
    } catch (IOException | TException e) {
      throw new DBException(e);
    }
  }

  public List<GenericRecord> get(DBContext context, Key key, List<String> groups) throws DBException {
    List<GenericRecord> ret = null;
    try {
      List<ByteBuffer> raw = iface.get(context, key, groups);
      Preconditions.checkState(raw == null || raw.size() == groups.size());
      if (raw != null) {
        ret = new ArrayList<>(raw.size());
        for (int i = 0; i < raw.size(); i++) {
          GenericRecord rec = null;
          byte[] bytes = ByteBufferUtil.toByteArray(raw.get(i));
          if (bytes != null) {
            org.apache.avro.Schema groupAvroSchema = getGroupAvroSchema(context, groups.get(i));
            if (groupAvroSchema != null) {
              rec = AvroUtil.fromBytes(bytes, groupAvroSchema);
            }
          }
          ret.add(rec);
        }
      }
    } catch (IOException | TException e) {
      throw new DBException(e);
    }

    Preconditions.checkState(ret == null || ret.size() == groups.size());
    return ret;
  }

  public void remove(DBContext context, Key key, List<String> groups) throws DBException {
    try {
      iface.remove(context, key, groups);
    } catch (TException e) {
      throw new DBException(e);
    }
  }
}
